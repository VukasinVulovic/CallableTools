from abc import abstractmethod
import asyncio
from dataclasses import dataclass
from enum import Enum
import logging
import re
import sys
from urllib.parse import unquote
from typing import Awaitable, Callable
import aio_pika
from aiomqtt import Client as MQTTClient, MqttError
from uuid import uuid4


if sys.platform.startswith("win"): #Windows bs :D
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class BrokerType(Enum):
    MQTT = "mqtt"
    AMQP = "amqp"
    AMQPS = "amqps"

@dataclass
class BrokerConnectionString:
    host: str
    port: int
    type: BrokerType
    username: str | None
    password: str | None

    def __init__(self, conn_str: str):
        m = re \
            .compile(
                r'^(?P<scheme>mqtt|amqp[s]?)://'
                r'(?:(?P<user>[^:/@]+)(?::(?P<password>[^@/]+))?@)?'
                r'(?P<host>[^:/]+)'
                r'(?::(?P<port>\d+))?'
                r'(?:/(?P<path>[^/]+))?$'
            ) \
            .match(conn_str) \
            .groupdict() \
            .items()
        
        parsed = { k: unquote(v) if v else None for k, v in m }

        self.type = BrokerType(parsed.get("scheme"))
        self.host = parsed.get("host")
        self.port = int(parsed.get("port", 1883))
        self.username = parsed.get("user")
        self.password = parsed.get("password")

@dataclass(eq=True, frozen=False)
class _Subscription:
    topic: str
    qos: int
    pending: bool
    
    def __hash__(self):
        return hash(self.topic)

class BrokerException(Exception):
    pass

@dataclass
class _AMQP:
    conn: aio_pika.abc.AbstractRobustConnection
    exchange: aio_pika.abc.AbstractRobustExchange
    channel: aio_pika.abc.AbstractRobustChannel
    queue: aio_pika.abc.AbstractRobustQueue

class MessagingBroker():
    __mqtt: MQTTClient = None
    __mqtt_connected = False
    __on_message_cb: Callable[[str, bytes], Awaitable[None]] | None = None
    __connection_lock = asyncio.Lock()
    __amqp: _AMQP = None
    __conn_str: BrokerConnectionString
    client_id: str
    __mqtt_connect_task: asyncio.Task = None
    __msg_task: asyncio.Task = None
    __subscriptons: set[_Subscription] = set()
    
    @property
    def type(self):
        return self.__conn_str.type
    
    def __init__(self, conn_str: BrokerConnectionString, client_id: str = None):
        self.__conn_str = conn_str
        self.client_id = client_id if client_id is not None else str(uuid4())
                
        if conn_str.type == BrokerType.MQTT:
            self.__mqtt = MQTTClient(
                conn_str.host, 
                conn_str.port, 
                clean_session=False,
                client_id=self.client_id, 
                password=conn_str.password, 
                username=conn_str.username
            )
            
    async def is_connected(self) -> bool:
        if self.type == BrokerType.MQTT:
            return self.__mqtt_connected
        elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
            return not await self.__amqp.conn.closed()

        return False
    
    async def subscribe(self, topic: str, qos: int = 2) -> None:
        topic = topic.replace(".", "/") if self.type == BrokerType.MQTT else topic.replace("/", ".")

        sub = _Subscription(topic=topic, qos=qos, pending=True)

        if sub in self.__subscriptons:
            return

        self.__subscriptons.add(sub)

        try:
            if await self.is_connected():
                await self.__subscribe_pending()
        except (MqttError, aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
        
    async def publish(self, topic: str, payload: bytes, retain: bool = True, qos:int=2) -> None:
        try:
            if self.__conn_str.type == BrokerType.MQTT:
                topic = topic.replace(".", "/")
                await self.__mqtt.publish(topic=topic, qos=qos, retain=retain, payload=payload)
            elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
                topic = topic.replace("/", ".")
                msg = aio_pika.Message(payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT if qos > 1 else aio_pika.DeliveryMode.NOT_PERSISTENT)
                await self.__amqp.exchange.publish(message=msg, routing_key=topic)
                
            logging.info(f"Published @ {topic}")
        except (MqttError, aio_pika.exceptions.AMQPError) as e:
            raise BrokerException(e)
    
    def __pending_subs(self):
        return (sub for sub in self.__subscriptons if sub.pending)
    
    async def __subscribe_pending(self):
        for sub in self.__pending_subs():
            if self.__conn_str.type == BrokerType.MQTT:
                await self.__mqtt.subscribe(topic=sub.topic, qos=sub.qos)
                sub.pending = False
                logging.info(f"Subscribed @ {sub.topic}")
            elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
                await self.__amqp.queue.bind(self.__amqp.exchange, routing_key=sub.topic)
    
    async def __mqtt_connect_loop(self):
        while True:
            try:                 
                if not await self.is_connected():
                    await self.__mqtt.connect()
                    self.__mqtt_connected = True
                    logging.info(f"Connected to broker as: {self.client_id}")
                    
                    await self.__subscribe_pending()
                    self.__msg_task = asyncio.create_task(self.__message_loop())
            except MqttError as e:
                self.__mqtt_connected = False
                for sub in self.__subscriptons:
                    sub.pending = True
                    
                logging.exception(e)
                
            except Exception as e:
                logging.exception(e)
                raise
            
            await asyncio.sleep(3)

    async def __message_loop(self):
        if self.__on_message_cb is None or not await self.is_connected():
            return
        
        logging.info("Ready to receive messages")
        
        async with self.__mqtt.messages() as queue:
            async for msg in queue:
                topic = str(msg.topic).replace("/", ".")
                await self.__on_message_cb(topic, msg.payload)

    @property
    @abstractmethod
    def on_message_cb(self):
        raise AttributeError("write-only")

    @on_message_cb.setter
    def on_message_cb(self, cb: Callable[[str, bytes], Awaitable[None]]) -> None:
        self.__on_message_cb = cb
        
        self.__msg_task = asyncio.create_task(self.__message_loop()).add_done_callback(lambda t: logging.info(t))

    async def connect(self) -> None:
        async with self.__connection_lock:
            if self.type == BrokerType.MQTT:
                if await self.is_connected():
                    return
                
                self.__mqtt_connect_task = asyncio.create_task(self.__mqtt_connect_loop())
                
    async def disconnect(self) -> None:
        async with self.__connection_lock:
            if not await self.is_connected():
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