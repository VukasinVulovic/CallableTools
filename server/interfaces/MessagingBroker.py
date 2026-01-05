import asyncio
from dataclasses import dataclass
from enum import Enum
from functools import cache
from hashlib import md5
import json
import logging
import sys
import aio_pika
from aiomqtt import Client as MQTTClient, MqttError

from common.exceptions import CouldNotParseToolRequestException, ToolNotFoundException, ToolValidationException
from common.helpers.connStringParser import BrokerConnectionString, BrokerType
from server.interfaces.baseInterface import ToolboxInterface
from common.models import DiscoveryRequest, DiscoveryResponse, RunToolRequest, ToolResponse, ToolStatus
from server.tooling import ToolBox

if sys.platform.startswith("win"): #Windows bs :D
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BrokerException(Exception):
    pass

class _MQTTTopics(Enum):
    DISCOVERY = "$share/discover"
    EXECUTE = "$share/execute"
    RESPONSE = "response"
    

@dataclass(eq=True, frozen=False)
class _MQTTSubscription:
    topic: str
    qos: int
    pending: bool
    
    def __hash__(self):
        return hash(self.topic)
    
@dataclass(eq=True, frozen=False)
class _AMQPSubscription:
    routing_key: str
    pending: bool
    
    def __hash__(self):
        return hash(self.routing_key)

class MQTTInterface(ToolboxInterface):
    _tool_box: ToolBox
    __mqtt: MQTTClient = None
    __is_connected: bool = False
    __connect_task: asyncio.Task = None
    __msg_task:  asyncio.Task = None
    __connection_lock = asyncio.Lock()
    __subscriptions = set[_MQTTSubscription]()
    __logger: logging.Logger
    
    def __init__(self, tb: ToolBox, conn_str: BrokerConnectionString):
        self._tool_box = tb
        self.__logger = logging.getLogger(tb.name)
        
        if conn_str.type is not BrokerType.MQTT:
            raise BrokerException("Supplied connection string is of type MQTT!")
         
        self.__mqtt = MQTTClient(
            conn_str.host, 
            conn_str.port, 
            clean_session=False,
            client_id=f"{tb.name}-{md5(__file__.encode()).digest().hex()}", 
            password=conn_str.password, 
            username=conn_str.username
        )
         
    @property
    def get_toolbox(self) -> ToolBox: self._tool_box
         
    async def open(self) -> None:
        async with self.__connection_lock:
            if self.__is_connected:
                return
            
            if self.__connect_task:
                self.__connect_task.cancel()
            
            self.__connect_task = asyncio.create_task(self.__connect_loop())
    
    async def close(self) -> None:
        if self.__is_connected:
            self.__mqtt.disconnect()
            
        if self.__connect_task:
            self.__connect_task.cancel()
    
    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        return False
    
    async def __send_tool_response(self, res: ToolResponse):
        await self.__mqtt.publish(f"{_MQTTTopics.RESPONSE.value}/{res.request_id}", payload=res.model_dump_json(), retain=True, qos=2)
    
    async def __respond_to_execute(self, topic: str, msg: str) -> None:
        if not self.__is_connected:
            return
                
        try:
            #parse from topic and message
            _, *tool = topic[len(_MQTTTopics.EXECUTE.value)+1:].split("/")
            
            async for res in self._tool_box.handle_raw_request(tool=".".join(tool), raw_request=msg):
                await self.__send_tool_response(res)
            
        except Exception as e:
            self.__logger.exception(e)
    
    @cache
    def __generate_introspection(self) -> str:
        return DiscoveryResponse(
            execute_schema=f"{_MQTTTopics.EXECUTE.value}/{{tool_box_name}}/{{callable_path}}",
            response_schema=f"{_MQTTTopics.RESPONSE.value}/{{request_id}}",
            tool_box_schema=json.loads(str(self._tool_box.__schema__)),
            interface="mqtt"
        ).model_dump_json()
    
    async def __handle_message(self, topic: str, msg: str):
        self.__logger.info(f"NEW Message at {topic}")
        
        try:
        
            match topic:
                case _MQTTTopics.DISCOVERY.value:
                    request_id = DiscoveryRequest.model_validate_json(msg).request_id
                    await self.__mqtt.publish(topic=f"{_MQTTTopics.RESPONSE.value}/{request_id}", payload=self.__generate_introspection())
                # case _MQTTTopics.DISCOVERY.
                case topic if topic.startswith(_MQTTTopics.EXECUTE.value):
                    # req.tool_box_name = topic
                    await self.__respond_to_execute(topic, msg)
        
        
        except (TypeError, AttributeError, ValueError) as e:
            self.__logger.warning(f"User request invalid: {e}")
        except Exception as e:
            self.__logger.exception(e)
    
    async def __message_loop(self): #loop to process mqtt messages
        if not self.__is_connected:
            return
                    
        async with self.__mqtt.messages() as queue:
            async for msg in queue:
                #asyncio.create_task(self.__handle_message(str(msg.topic), msg.payload.decode()))
                await self.__handle_message(str(msg.topic), msg.payload.decode())
    
    async def __subscribe(self): #process pending queue creations and subscriptions
        try:
            for tool in self._tool_box.tools:
                sub = _MQTTSubscription(topic=f"{_MQTTTopics.EXECUTE.value}/{self._tool_box.name}/{tool}", pending=True, qos=2)
                self.__subscriptions.add(sub)
                
                await self.__mqtt.subscribe(topic=sub.topic, qos=sub.qos)
                sub.pending = False
            
            await self.__mqtt.subscribe(topic=_MQTTTopics.DISCOVERY.value, qos=0)
                
        except (MqttError, aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        except Exception as e:
            pass
    
    async def __connect_loop(self): #auto-reconnect for mqtt
        while True:
            try:                 
                if not self.__is_connected:
                    await self.__mqtt.connect()
                    self.__is_connected = True
                    
                    await self.__subscribe()
                    
                    if self.__msg_task:
                        self.__msg_task.cancel()
                    
                    self.__msg_task = asyncio.create_task(self.__message_loop())
            except MqttError as e:
                self.__is_connected = False
                for sub in self.__subscriptons:
                    sub.pending = True
                    
                logging.exception(e)
                
            except Exception as e:
                logging.exception(e)
                raise
            
            await asyncio.sleep(3)
      
class _AMQPExchanges(Enum):
    DISCOVERY = "discover"
    EXECUTE = "execute"      
          
@dataclass 
class _AMQP:
    conn: aio_pika.abc.AbstractRobustConnection
    channel: aio_pika.abc.AbstractRobustChannel
    execute_exchange: aio_pika.abc.AbstractRobustExchange
    discovery_exchange: aio_pika.abc.AbstractRobustExchange
 
class AMQPInterface(ToolboxInterface):
    _tool_box: ToolBox
    __amqp: _AMQP = None
    __connection_lock = asyncio.Lock()
    __subscriptions = set[_MQTTSubscription]()
    __logger: logging.Logger
    __conn_str: BrokerConnectionString
    
    def __init__(self, tb: ToolBox, conn_str: BrokerConnectionString):
        self.__conn_str = conn_str
        self._tool_box = tb
        self.__logger = logging.getLogger(tb.name)
        
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
            
            await self.__subscribe()
    
    async def close(self) -> None:
        if self.__amqp and not self.__amqp.conn.is_closed:
            self.__amqp.channel.close()
            self.__amqp.conn.close()
            
    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        return False
    
    async def __respond_to_execute(self, msg: aio_pika.abc.AbstractIncomingMessage) -> None:        
        if not self.__amqp or self.__amqp.conn.is_closed:
            return
                
        try:
            #parse from topic and message
            _, *tool = msg.routing_key[len(_AMQPExchanges.EXECUTE.value)+1:].split(".")
            
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
            execute_schema=f"{_AMQPExchanges.EXECUTE.value}/{{tool_box_name}}/{{callable_path}}",
            response_schema=f"{{reply to}}",
            tool_box_schema=json.loads(str(self._tool_box.__schema__)),
            interface="mqtt"
        ).model_dump_json().encode()
    
    async def __handle_message(self, msg: aio_pika.abc.AbstractIncomingMessage):
        exc = msg.exchange if len(msg.exchange) > 0 else msg.routing_key.split(".")[0]
        
        self.__logger.info(f"NEW Message at {exc}")
        
        async with msg.process():
            if not msg.reply_to:   # not an RPC message
                return
        
            try:
            
                match exc:
                    case _AMQPExchanges.DISCOVERY.value:
                        await self.__amqp.channel.default_exchange.publish(
                            aio_pika.Message(
                                body=self.__generate_introspection(),
                                correlation_id=msg.correlation_id,
                            ),
                            routing_key=msg.reply_to
                        )
                        
                    case _AMQPExchanges.EXECUTE.value:
                        await self.__respond_to_execute(msg)
            
            except (TypeError, AttributeError, ValueError) as e:
                self.__logger.warning(f"User request invalid: {e}")
            except Exception as e:
                self.__logger.exception(e)
    
    async def __subscribe(self): #process pending queue creations and subscriptions
        try:
            for tool in self._tool_box.tools:
                sub = _AMQPSubscription(routing_key=f"{_AMQPExchanges.EXECUTE.value}.{self._tool_box.name}.{tool}", pending=True)
                self.__subscriptions.add(sub)
                
                q = await self.__amqp.channel.declare_queue(name=sub.routing_key, passive=False, durable=True)
                await q.bind(self.__amqp.execute_exchange, routing_key=sub.routing_key, robust=True)
                await q.consume(self.__handle_message)
                
                sub.pending = False
            
            dq = await self.__amqp.channel.declare_queue(name=_AMQPExchanges.DISCOVERY.value, passive=False, durable=False)
            await dq.bind(self.__amqp.discovery_exchange, routing_key=f"{_AMQPExchanges.DISCOVERY.value}#")
            await dq.consume(self.__handle_message, no_ack=False)
                
        except (aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        except Exception as e:
            pass