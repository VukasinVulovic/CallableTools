from abc import abstractmethod
import asyncio
from dataclasses import dataclass
import logging
import sys
from typing import Awaitable, Callable 
import aio_pika
from aiomqtt import Client as MQTTClient, MqttError
from uuid import uuid4

if sys.platform.startswith("win"): #Windows bs :D
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@dataclass(eq=True, frozen=False)
class _Subscription:
    topic: str
    qos: int
    pending: bool
    is_brodcast: bool
    
    def __hash__(self):
        return hash(self.topic)
    
@dataclass(eq=True, frozen=False)
class _Queue:
    name: str
    pending: bool
    
    def __hash__(self):
        return hash(self.name)

class BrokerException(Exception):
    pass

@dataclass
class _AMQP:
    conn: aio_pika.abc.AbstractRobustConnection
    exchange: aio_pika.abc.AbstractRobustExchange
    channel: aio_pika.abc.AbstractRobustChannel

class BrokerHandler():
    __mqtt: MQTTClient = None
    __mqtt_connected = False
    __on_message_cb: Callable[[str, bytes], Awaitable[None]] | None = None
    __connection_lock = asyncio.Lock()
    __amqp: _AMQP = None
    __conn_str: BrokerConnectionString
    __client_id: str
    __mqtt_connect_task: asyncio.Task = None
    __msg_task: asyncio.Task = None
    __subscriptons: set[_Subscription] = set()
    __queues: set[_Queue] = set()
    
    @property
    def type(self):
        return self.__conn_str.type
    
    def __init__(self, conn_str: BrokerConnectionString, client_id: str = None):
        self.__conn_str = conn_str
        self.__client_id = client_id if client_id is not None else str(uuid4())
                
        if conn_str.type == BrokerType.MQTT:
            self.__mqtt = MQTTClient(
                conn_str.host, 
                conn_str.port, 
                clean_session=False,
                client_id=self.__client_id, 
                password=conn_str.password, 
                username=conn_str.username
            )
            
    @property
    def is_connected(self) -> bool:
        if self.type == BrokerType.MQTT:
            return self.__mqtt_connected
        elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
            return not self.__amqp.conn.is_closed if self.__amqp and self.__amqp.conn else False

        return False
    
    @staticmethod
    def __normalize_topic(topic: str): #replace mqtt syntax to amqp to standardize
        return \
            topic \
            .replace("/", ".") \
            .replace("+", "#") \
            .replace("$share.", "")
    
    def __convert_topic(self, topic: str):
        if self.__conn_str.type == BrokerType.MQTT: #for mqtt, add share so other consumers can process messages, also convert to correct syntax for mqtt
            return \
                (f"$share/{topic}" if not topic.startswith("$share") else topic) \
                .replace(".", "/") \
                .replace("#", "+")
        elif self.type in (BrokerType.AMQP, BrokerType.AMQPS): #for amqp, remove share, convet mqtt syntax to amqp
            return \
                topic \
                .replace("/", ".") \
                .replace("+", "#") \
                .replace("$share.", "")
    
        return topic
    
    async def create_wild_queue(self, name: str) -> None:
        if self.type in (BrokerType.AMQP, BrokerType.AMQPS):
            self.__queues.add(_Queue(self.__convert_topic(name), True))
            
        if self.is_connected:
            self.__process_pending()
        
    async def subscribe(self, topic: str, qos: int = 2, is_brodcast: bool = False) -> None: #add to pending
        sub = _Subscription(topic=self.__convert_topic(topic), qos=qos, pending=True, is_brodcast=is_brodcast)

        if sub in self.__subscriptons:
            return

        self.__subscriptons.add(sub)
        
        if self.is_connected: #if connected to broker, apply subscription
            await self.__process_pending()     
          
    async def publish(self, topic: str, payload: bytes, retain: bool=True, qos:int=2) -> None: #publish to topic
        topic = self.__convert_topic(topic).replace("$share/", "")
        
        try:
            if self.__conn_str.type == BrokerType.MQTT:
                await self.__mqtt.publish(topic=topic, qos=qos, retain=retain, payload=payload)
            elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
                msg = aio_pika.Message(payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT if qos > 1 else aio_pika.DeliveryMode.NOT_PERSISTENT)
                await self.__amqp.exchange.publish(message=msg, routing_key=topic)
        except (MqttError, aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
    
    async def __process_pending(self): #process pending queue creations and subscriptions
        try:
            if self.type in (BrokerType.AMQP, BrokerType.AMQPS): #create pending queues for amqp
                for queue in (q for q in self.__queues if q.pending):
                    q = await self.__amqp.channel.declare_queue(queue.name, passive=False, exclusive=False, durable=True)
                    await q.bind(exchange=self.__amqp.exchange, routing_key=queue.name)
            
            for sub in (sub for sub in self.__subscriptons if sub.pending):
                if self.__conn_str.type == BrokerType.MQTT:
                    await self.__mqtt.subscribe(topic=sub.topic, qos=sub.qos)
                elif self.type in (BrokerType.AMQP, BrokerType.AMQPS): #for amqp, delcare queue, bind and then define consumer
                    queue = await self.__amqp.channel.declare_queue(sub.topic, passive=False, exclusive=False, durable=True)
                    await queue.bind(exchange=self.__amqp.exchange, routing_key=sub.topic)
                    await queue.consume(self.__process_amqp_message)
                    
                sub.pending = False
        except (MqttError, aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)   
    
    async def __mqtt_connect_loop(self): #auto-reconnect for mqtt
        while True:
            try:                 
                if not self.is_connected:
                    await self.__mqtt.connect()
                    self.__mqtt_connected = True
                    
                    await self.__process_pending()
                    
                    if self.__msg_task:
                        self.__msg_task.cancel()
                    
                    self.__msg_task = asyncio.create_task(self.__mqtt_message_loop())
            except MqttError as e:
                self.__mqtt_connected = False
                for sub in self.__subscriptons:
                    sub.pending = True
                    
                logging.exception(e)
                
            except Exception as e:
                logging.exception(e)
                raise
            
            await asyncio.sleep(3)

    async def __mqtt_message_loop(self): #loop to process mqtt messages
        if self.type == BrokerType.MQTT:
            if self.__on_message_cb is None or not self.is_connected:
                return
                        
            async with self.__mqtt.messages() as queue:
                async for msg in queue:
                    topic = self.__normalize_topic(str(msg.topic))
                    asyncio.create_task(self.__on_message_cb(topic, msg.payload))
 
    async def __process_amqp_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None: #internal callback to process amqp message
        async with message.process():
            if self.__on_message_cb and self.is_connected:            
                asyncio.create_task(self.__on_message_cb(message.routing_key, message.body))

    @property
    @abstractmethod
    def on_message_cb(self):
        raise AttributeError("write-only")

    @on_message_cb.setter #callback that will be called when message is received
    def on_message_cb(self, cb: Callable[[str, bytes], Awaitable[None]]) -> None:
        self.__on_message_cb = cb
        
        if self.type == BrokerType.MQTT:
            self.__msg_task = asyncio.create_task(self.__mqtt_message_loop())

    async def connect(self) -> None: #connect to broker, for mqtt use connection loop
        async with self.__connection_lock:
            if self.type == BrokerType.MQTT:
                if self.is_connected:
                    return
                
                self.__mqtt_connect_task = asyncio.create_task(self.__mqtt_connect_loop())
            
            elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
                if self.is_connected:
                    return
                
                conn = await aio_pika.connect_robust(
                    host=self.__conn_str.host, 
                    port=self.__conn_str.port, 
                    login=self.__conn_str.username, 
                    password=self.__conn_str.password, 
                    ssl=self.type == BrokerType.AMQPS
                )
                                                
                channel = await conn.channel()
                exchange = await channel.declare_exchange(
                    self.__client_id.split(".")[0] if "." in self.__client_id else str(uuid4()), 
                    type=aio_pika.ExchangeType.TOPIC, 
                    durable=True, 
                    passive=False
                )
                
                self.__amqp = _AMQP(
                    conn=conn,
                    channel=channel,
                    exchange=exchange
                )
                
                await self.__process_pending()
                           
    async def disconnect(self) -> None: #cancel all tasks and disconnect
        async with self.__connection_lock:
            if not self.is_connected:
                return
            
            if self.__msg_task:
                self.__msg_task.cancel()
            
            if self.type == BrokerType.MQTT:        
                self.__mqtt_connect_task.cancel()
                
                await self.__mqtt.disconnect()
                self.__mqtt_connected = False
            
            elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
                await self.__amqp.conn.close()
                  
            self.__subscriptons.clear()