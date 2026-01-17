import asyncio
from dataclasses import dataclass
from functools import cache
from hashlib import md5
from itertools import chain
import logging
import uuid

import aio_pika

from ...common.exceptions import ToolNotFoundException, ToolRuntimeException, ToolValidationException
from ...common.helpers.connStringParser import BrokerConnectionString, BrokerType
from ...common.models import DiscoveryResponse, RunToolRequest, ToolResponse, ToolStatus
from ...common.routes import _AMQPRoutes
from ...common.schema import Tool, ToolBox

class BrokerException(Exception):
    pass

@dataclass 
class _AMQP:
    conn: aio_pika.abc.AbstractRobustConnection
    channel: aio_pika.abc.AbstractRobustChannel
    execute_exchange: aio_pika.abc.AbstractRobustExchange
    discovery_exchange: aio_pika.abc.AbstractRobustExchange
    updates_exchange: aio_pika.abc.AbstractRobustExchange

#AMQP based
class AMQPClient:
    __amqp: _AMQP = None
    __connection_lock = asyncio.Lock()
    __logger: logging.Logger
    __conn_str: BrokerConnectionString
    __discoveries: set[DiscoveryResponse] = set()
    __tasks: list[asyncio.Task] = []
    __discovery_lock: asyncio.Lock = asyncio.Lock()
    tools_discovered_ev: asyncio.Event = asyncio.Event()
    DISCOVERY_REPLY_TIMEOUT = 3
    TOOL_REPLY_TIMEOUT=120
    MAX_DISCOVERIES=2
    
    @property
    def toolboxes(self) -> list[ToolBox]: return list(map(lambda d: d.tool_box_schema, self.__discoveries))
    
    def __init__(self, conn_str: BrokerConnectionString):
        self.__conn_str = conn_str
        self.__logger = logging.getLogger(f"{conn_str.type.name} - {conn_str.host}:{conn_str.port}")
        
        if conn_str.type not in (BrokerType.AMQP, BrokerType.AMQPS):
            raise BrokerException("Supplied connection string is of type AMQP/AMQPS!")
            
    async def open(self) -> None:
        async with self.__connection_lock:
            if self.__amqp and not self.__amqp.conn.is_closed:
                return
            
            conn = await aio_pika.connect_robust(
                host=self.__conn_str.host, 
                port=self.__conn_str.port, 
                login=self.__conn_str.username, 
                password=self.__conn_str.password, 
                ssl=self.__conn_str.type == BrokerType.AMQPS
            )
                                            
            channel = await conn.channel()
            
            execute_exc = await channel.declare_exchange(
                name=_AMQPRoutes.EXECUTE.value, 
                type=aio_pika.ExchangeType.TOPIC, 
                durable=True, 
                passive=False
            )
            
            discover_exc = await channel.declare_exchange(
                name=_AMQPRoutes.DISCOVERY.value, 
                type=aio_pika.ExchangeType.FANOUT, 
                durable=False, 
                passive=False,
                auto_delete=True
            )
            
            updates_exc = await channel.declare_exchange(
                name=_AMQPRoutes.UPDATES.value, 
                type=aio_pika.ExchangeType.FANOUT, 
                durable=False, 
                passive=False,
                auto_delete=True
            )
            
            self.__amqp = _AMQP(
                conn=conn,
                channel=channel,
                discovery_exchange=discover_exc,
                execute_exchange=execute_exc,
                updates_exchange=updates_exc
            )
            
            #subscribe to updates
            self.__tasks.append(asyncio.create_task(self.__subscribe_updates()))

            #subscribe to discovery
            self.__tasks.append(asyncio.create_task(self.fetch_discovery(self.MAX_DISCOVERIES)))

    @cache
    def __get_discovered_tool(self, tool_box_name: str, tool_name: str) -> tuple[DiscoveryResponse, Tool]:
        discovered: DiscoveryResponse = next((d for d in self.__discoveries if d.tool_box_schema.name == tool_box_name), None)
        
        if discovered is None:
            raise ToolNotFoundException()
        
        tools = list(chain.from_iterable(tg.tools for tg in discovered.tool_box_schema.tool_groups))
        tool:Tool = next((t for t in tools if t.name == tool_name), None)
        
        if tool is None:
            raise ToolNotFoundException()
        
        return (discovered, tool)

    async def execute(self, tool_box_name: str, tool_name: str, params: dict={}) -> object | None:
        #TODO: validate from discovered tools
        
        (discovery, tool) = self.__get_discovered_tool(tool_box_name, tool_name)
        
        req_id = uuid.uuid4()
        req = RunToolRequest(
            tool_box_name=tool_box_name,
            tool_name=tool.name,
            parameters=params,
            stop_on_fail=True,
            request_id=req_id
        )
        
        routing_key = discovery.execute_schema\
            .replace("{callable_path}", tool.callable_path)\
            .replace("/", ".")
        
        try:            
            reply_queue = await self.__request(
                exc=self.__amqp.execute_exchange, 
                routing_key=routing_key,
                req_id=req_id,
                body=req.model_dump_json(),
            )
            
            async with reply_queue.iterator() as i:
                while True:
                    message = await asyncio.wait_for(
                        i.__anext__(),
                        timeout=self.TOOL_REPLY_TIMEOUT,
                    )

                    async with message.process():                                            
                        res = ToolResponse.model_validate_json(message.body)
                        
                        if res.request_id != req_id:
                            continue
                        
                        match res.status:
                            case ToolStatus.REJECTED:
                                raise ToolValidationException(res.output)
                            case ToolStatus.ACCEPTED: #TODO: handle accept
                                pass
                            case ToolStatus.PROCESSED:
                                if not res.success:
                                    raise ToolRuntimeException(res.output)
                                
                                return res.output
        except (aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        except (TypeError, ValueError) as e:
            self.__logger.warning(f"Received invalid tool response: {e}")
        except Exception:
            raise
            
        return None
        
    async def __on_update(self, msg: aio_pika.abc.AbstractIncomingMessage):
        exc = msg.exchange if len(msg.exchange) > 0 else msg.routing_key.split(".")[0]
        
        self.__logger.info(f"NEW update!")
        
        async with msg.process():
            try:
                match exc:
                    case _AMQPRoutes.UPDATES.value:
                        self.__discoveries.update()
                        self.__discoveries.add(DiscoveryResponse.model_validate_json(msg.body))  
                        self.tools_discovered_ev.set()
            
            except (TypeError, AttributeError, ValueError) as e:
                self.__logger.warning(f"Request invalid: {e}")
            except Exception as e:
                self.__logger.exception(e)
        
    async def __subscribe_updates(self) -> None:
        async with self.__discovery_lock:
            try:
                #subscribe to tool changes
                q = await self.__amqp.channel.declare_queue(name=_AMQPRoutes.UPDATES.value, passive=False, durable=False, auto_delete=True)
                await q.bind(self.__amqp.execute_exchange, routing_key=_AMQPRoutes.UPDATES.value, robust=False)
                await q.consume(self.__on_update)                            
            except (aio_pika.exceptions.AMQPError) as e:
                raise BrokerException(e)
            except (TypeError, ValueError) as e:
                self.__logger.warning(f"Received invalid updates response: {e}")
            except Exception:
                raise

    async def fetch_discovery(self, max_tbs: int) -> set[DiscoveryResponse]:
        async with self.__discovery_lock:
            self.__logger.info("Running toolbox discovery...")
            req_id = uuid.uuid4()
            discovered = set[DiscoveryResponse]() #somehow convert to set
            
            try:            
                reply_queue = await self.__request(
                    exc=self.__amqp.discovery_exchange, 
                    routing_key="",
                    req_id=req_id,
                    body=f'{{"request_id": "{req_id}"}}',
                )
                
                async with reply_queue.iterator() as i:
                    while len(discovered) < max_tbs:
                        try:
                            message = await asyncio.wait_for(
                                i.__anext__(),
                                timeout=self.DISCOVERY_REPLY_TIMEOUT,
                            )

                            async with message.process():                                            
                                discovered.add(DiscoveryResponse.model_validate_json(message.body))    
                        except asyncio.TimeoutError:
                            self.__discoveries = discovered
                            self.tools_discovered_ev.set()
                            return discovered                               
            except (aio_pika.exceptions.AMQPError) as e:
                raise BrokerException(e)
            except (TypeError, ValueError) as e:
                self.__logger.warning(f"Received invalid discovery response: {e}")
            except Exception:
                raise
                
            self.__discoveries = discovered
            self.tools_discovered_ev.set()
            return discovered
  
    async def __request(self, exc: aio_pika.abc.AbstractRobustExchange, req_id: uuid, routing_key: str, body: bytes | str) -> aio_pika.abc.AbstractRobustQueue:
        #create reply queue
        reply_queue = f"reply.{md5(__file__.encode()).digest().hex()}.{req_id}"
        
        q = await self.__amqp.channel.declare_queue(
            name=reply_queue,
            durable=False,
            exclusive=True,
            auto_delete=True,
            passive=False
        )
                
        #fetch
        
        await exc.publish(routing_key=routing_key, message=aio_pika.Message(
            body=body if type(body) is bytes else body.encode(),
            correlation_id=req_id,
            reply_to=reply_queue
        ))
        
        return q
    
    async def close(self) -> None:
        for t in self.__tasks:
            t.cancel()
        
        if self.__amqp and not self.__amqp.conn.is_closed:
            await self.__amqp.channel.close()
            await self.__amqp.conn.close()
            
    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        return False      