import asyncio
from dataclasses import dataclass
from functools import cache
from hashlib import md5
from itertools import chain
import logging
from typing import AsyncGenerator
import uuid

import aio_pika

from common.exceptions import ToolNotFoundException, ToolRuntimeException, ToolValidationException
from common.helpers.connStringParser import BrokerConnectionString, BrokerType
from common.models import DiscoveryResponse, RunToolRequest, ToolResponse, ToolStatus
from common.routes import _AMQPExchanges
from common.schema import Tool, ToolBox

class BrokerException(Exception):
    pass

@dataclass(eq=True, frozen=False)
class _AMQPSubscription:
    routing_key: str
    pending: bool
    
    def __hash__(self):
        return hash(self.routing_key)

@dataclass 
class _AMQP:
    conn: aio_pika.abc.AbstractRobustConnection
    channel: aio_pika.abc.AbstractRobustChannel
    execute_exchange: aio_pika.abc.AbstractRobustExchange
    discovery_exchange: aio_pika.abc.AbstractRobustExchange
    
@dataclass(eq=True, frozen=False)
class _AMQPSubscription:
    routing_key: str
    pending: bool
    
    def __hash__(self):
        return hash(self.routing_key)


#AMQP based
class AMQPClient:
    __amqp: _AMQP = None
    __connection_lock = asyncio.Lock()
    __subscriptions = set[_AMQPSubscription]()
    __logger: logging.Logger
    __conn_str: BrokerConnectionString
    __discoveries: list[DiscoveryResponse] = []
    DISCOVERY_REPLY_TIMEOUT = 3
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
                name=_AMQPExchanges.EXECUTE.value, 
                type=aio_pika.ExchangeType.TOPIC, 
                durable=True, 
                passive=False
            )
            
            discover_exc = await channel.declare_exchange(
                name=_AMQPExchanges.DISCOVERY.value, 
                type=aio_pika.ExchangeType.FANOUT, 
                durable=False, 
                passive=False
            )
            
            self.__amqp = _AMQP(
                conn=conn,
                channel=channel,
                discovery_exchange=discover_exc,
                execute_exchange=execute_exc
            )
            
            #subscribe to discovery
  
            self.__logger.info("Runnint toolbox discovery...")
            self.__discoveries = await self.__fetch_discovery(self.MAX_DISCOVERIES)
            self.__logger.info(f"Discovered {len(self.__discoveries)} toolboxes.")

    @cache
    def __get_discovered_tool(self, tool_box_name: str, tool_name: str) -> tuple[DiscoveryResponse, Tool]:
        discovered: DiscoveryResponse = next((d for d in self.__discoveries if d.tool_box_schema.get("name") == tool_box_name), None)
        
        if discovered is None:
            raise ToolNotFoundException()
        
        tools = list(chain.from_iterable(tg.get("tools") for tg in discovered.tool_box_schema.get("tool_groups")))
        tool:Tool = next((t for t in tools if t.get("name") == tool_name), None)
        
        if tool is None:
            raise ToolNotFoundException()
        
        return (discovered, Tool(**tool))

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
                    try:
                        message = await asyncio.wait_for(
                            i.__anext__(),
                            timeout=self.DISCOVERY_REPLY_TIMEOUT,
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
                                    
                                    
                            
                            # discovery.append(DiscoveryResponse.model_validate_json(message.body))    
                    except asyncio.TimeoutError:
                        return discovery                               
        except (aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        except (TypeError, ValueError) as e:
            self.__logger.warning(f"Received invalid discovery response: {e}")
        except Exception:
            raise
            
        return None

    async def __fetch_discovery(self, max_tbs: int) -> list[DiscoveryResponse]:
        req_id = uuid.uuid4()
        discovered = list[DiscoveryResponse]() #somehow convert to set
        
        try:            
            reply_queue = await self.__request(
                exc=self.__amqp.discovery_exchange, 
                routing_key=_AMQPExchanges.DISCOVERY.value,
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
                            discovered.append(DiscoveryResponse.model_validate_json(message.body))    
                    except asyncio.TimeoutError:
                        return discovered                               
        except (aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        except (TypeError, ValueError) as e:
            self.__logger.warning(f"Received invalid discovery response: {e}")
        except Exception:
            raise
            
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
        if self.__amqp and not self.__amqp.conn.is_closed:
            await self.__amqp.channel.close()
            await self.__amqp.conn.close()
            
    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        return False      