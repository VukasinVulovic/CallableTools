import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from functools import cache

import aio_pika

from ...common.helpers.connStringParser import BrokerConnectionString, BrokerType
from ...common.models import DiscoveryRequest, DiscoveryResponse, ToolResponse
from ...common.routes import _AMQPRoutes
from ...server.tooling import ToolBox

if sys.platform.startswith("win"): #Windows bs :D
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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
    updates_exchange: aio_pika.abc.AbstractRobustExchange
 
class AMQPInterface():
    _tool_box: ToolBox
    __amqp: _AMQP = None
    __connection_lock = asyncio.Lock()
    __subscribtion_lock = asyncio.Lock()
    __subscriptions = set[_AMQPSubscription]()
    __logger: logging.Logger
    __conn_str: BrokerConnectionString
    __discovery_subscribed: bool = False
    
    def __init__(self, tb: ToolBox, conn_str: BrokerConnectionString):
        self.__conn_str = conn_str
        self._tool_box = tb
        self.__logger = logging.getLogger(tb.name)
        tb.on_change_cb = self.__send_updates
        
        if conn_str.type not in (BrokerType.AMQP, BrokerType.AMQPS):
            raise BrokerException("Supplied connection string is of type AMQP/AMQPS!")
         
    @property
    def get_toolbox(self) -> ToolBox: self._tool_box
         
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
            
            updates_q = await channel.declare_queue(
                name=_AMQPRoutes.UPDATES.value,
                auto_delete=True,
                durable=False,
                exclusive=False,
                passive=False
            )
            
            await updates_q.bind(updates_exc)
            
            self.__amqp = _AMQP(
                conn=conn,
                channel=channel,
                discovery_exchange=discover_exc,
                execute_exchange=execute_exc,
                updates_exchange=updates_exc
            )
            
            self.__logger.info("Connected and subscribing...")
            
            await self.__subscribe()
            self.__logger.info("Subscribed")
            
    
    async def close(self) -> None:
        if self.__amqp and not self.__amqp.conn.is_closed:
            await self.__amqp.channel.close()
            await self.__amqp.conn.close()
            
    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        self.__logger.info("Exited")
        return False
    
    async def __send_updates(self):
        if not self.__amqp or self.__amqp.conn.is_closed:
            return
        
        self.__logger.info("Sending schema update...")
        
        res = DiscoveryResponse(
            execute_schema=f"{_AMQPRoutes.EXECUTE.value}/{{callable_path}}",
            response_schema="{reply to}",
            tool_box_schema=json.loads(str(self._tool_box.__schema__)),
            updates_schema=_AMQPRoutes.UPDATES.value,
            interface="amqp"
        )
                        
        await self.__amqp.updates_exchange.publish(
            aio_pika.Message(
                body=res.model_dump_json().encode()
            ),
            routing_key=_AMQPRoutes.UPDATES.value
        )
    
    async def __respond_to_execute(self, msg: aio_pika.abc.AbstractIncomingMessage) -> None:        
        if not self.__amqp or self.__amqp.conn.is_closed:
            return
                
        try:
            #parse from topic and message
            _, *tool = msg.routing_key[len(_AMQPRoutes.EXECUTE.value)+1:].split(".")
            
            async for res in self._tool_box.handle_raw_request(tool=".".join(tool), raw_request=msg.body):
                await self.__amqp.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=res.model_dump_json().encode(),
                        correlation_id=msg.correlation_id,
                    ),
                    routing_key=msg.reply_to
                )
            
        except Exception as e:
            self.__logger.exception(e)
    
    @cache
    def __generate_introspection(self) -> str:
        return DiscoveryResponse(
            execute_schema=f"{_AMQPRoutes.EXECUTE.value}/{{callable_path}}",
            response_schema="{reply to}",
            tool_box_schema=json.loads(str(self._tool_box.__schema__)),
            updates_schema=_AMQPRoutes.UPDATES.value,
            interface="amqp"
        ).model_dump_json().encode()
    
    async def __handle_message(self, msg: aio_pika.abc.AbstractIncomingMessage):
        exc = msg.exchange if len(msg.exchange) > 0 else msg.routing_key.split(".")[0]
        
        self.__logger.info(f"NEW Message at {exc}")
        
        async with msg.process():
            if not msg.reply_to:   # not an RPC message
                return
        
            try:
            
                match exc:
                    case _AMQPRoutes.DISCOVERY.value:
                        await self.__amqp.channel.default_exchange.publish(
                            aio_pika.Message(
                                body=self.__generate_introspection(),
                                correlation_id=msg.correlation_id,
                            ),
                            routing_key=msg.reply_to
                        )
                        
                    case _AMQPRoutes.EXECUTE.value:
                        await self.__respond_to_execute(msg)
            
            except (TypeError, AttributeError, ValueError) as e:
                self.__logger.warning(f"User request invalid: {e}")
            except Exception as e:
                self.__logger.exception(e)
    
    async def __subscribe(self): #process pending queue creations and subscriptions
        if not self.__amqp or self.__amqp.conn.is_closed:
            return
        
        async with self.__subscribtion_lock:
            try:
                for tool in self._tool_box.tools:
                    sub = _AMQPSubscription(routing_key=f"{_AMQPRoutes.EXECUTE.value}.{self._tool_box.name}.{tool}", pending=True)
                    
                    if sub in self.__subscriptions:
                        continue
                    
                    self.__subscriptions.add(sub)
                    
                    q = await self.__amqp.channel.declare_queue(name=sub.routing_key, passive=False, durable=True, exclusive=False)
                    await q.bind(self.__amqp.execute_exchange, routing_key=sub.routing_key, robust=True)
                    await q.consume(self.__handle_message)
                    
                    sub.pending = False
                
                if not self.__discovery_subscribed:
                    dq = await self.__amqp.channel.declare_queue(name=_AMQPRoutes.DISCOVERY.value, passive=False, durable=False, exclusive=False)
                    await dq.bind(self.__amqp.discovery_exchange)
                    await dq.consume(self.__handle_message, no_ack=False)
                    self.__discovery_subscribed = True
                
            except (aio_pika.exceptions.AMQPError) as e:
                raise BrokerException(e)
            except Exception:
                pass