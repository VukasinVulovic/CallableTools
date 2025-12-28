from abc import abstractmethod
import asyncio
from dataclasses import dataclass
from enum import Enum
import logging
import re
import sys
import time
from urllib.parse import unquote
from typing import Awaitable, Callable
from aiomqtt import Client as MQTTClient, MqttError
from uuid import uuid4

from attr import frozen

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

@dataclass
class Message:
    topic: str
    payload: bytes

@dataclass
class Subscription:
    topic: str
    qos: int
    pending: bool
    
    def __hash__(self):
        return hash(self.topic)

class BrokerException(Exception):
    pass

class MessagingBroker():
    __mqtt: MQTTClient = None
    # __amqp: aio_pika.abc.AbstractRobustConnection = None
    __conn_str: BrokerConnectionString
    client_id: str
    __tasks: list[asyncio.Task] = []
    __subscriptons: set[Subscription] = set()
    
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
            return self.__mqtt._client.is_connected()
        # elif self.type in (BrokerType.AMQP, BrokerType.AMQPS):
        #     return not await self.__amqp.conn.closed()

        return False
    
    async def subscribe(self, topic: str, qos:int=2) -> None:
        sub = Subscription(topic, qos=qos, pending=True)
        
        if self.type == BrokerType.MQTT:
            topic = topic.replace("/", ".")
        
        if sub in self.__subscriptons:
            return
                    
        self.__subscriptons.add(sub)
        
        try:
            if await self.is_connected():
                await self.__subscribe_pending()
        
        except (MqttError) as e:
            raise BrokerException(e)
        
    async def publish(self, topic: str, payload: bytes, retain: bool = True, qos:int=2) -> None:
        if self.type == BrokerType.MQTT:
            topic = topic.replace("/", ".")
            
        try:
            if self.__conn_str.type == BrokerType.MQTT:
                return await self.__mqtt.publish(topic=topic, qos=qos, retain=retain, payload=payload)
        except (MqttError) as e:
            raise BrokerException(e)
        
    async def disconnect(self) -> None:
        if self.__conn_str.type == BrokerType.MQTT:
            return await self.__mqtt.disconnect()
        
        for sub in self.__subscriptons:
            self.__subscriptons.remove(sub)
    
    @property
    def connected(self) -> bool:
        if self.__conn_str.type == BrokerType.MQTT:
            return self.__mqtt._client.is_connected()

        return False
    
    async def __pending_subs(self):
        for sub in self.__subscriptons:
            if sub.pending:
                yield sub
    
    async def __subscribe_pending(self):        
        async for sub in self.__pending_subs():
            if self.__conn_str.type == BrokerType.MQTT:
                await self.__mqtt.subscribe(topic=sub[0], qos=sub[1])
    
    async def __mqtt_connect_loop(self):
        while True:
            try:                 
                if not self.__mqtt._client.is_connected():
                    await self.__mqtt.connect()
                    await self.__subscribe_pending()
                    
                    logging.info(f"Connected to broker as: {self.client_id}")
                    
            except MqttError as e:
                for sub in self.__subscriptons:
                    sub.pending = True
                    
                logging.exception(e)
                time.sleep(3)

    async def __mqtt_message_loop(self, cb):
        async with self.__mqtt.messages() as queue:
            async for msg in queue:
                topic = ".".join(str(msg.topic).replace("/", ".").split(".")[1:])
                await cb(topic, msg.payload)

    @property
    def on_message_cb(self):
        pass

    @property
    @abstractmethod
    def on_message_cb(self):
        raise AttributeError("write-only")

    @on_message_cb.setter
    def on_message_cb(self, cb: Callable[[str, bytes], Awaitable[None]]) -> None:
        if self.__conn_str.type == BrokerType.MQTT:
            self.__tasks.append(asyncio.create_task(self.__mqtt_message_loop(cb)))

    async def connect(self) -> None:
        if self.__conn_str.type == BrokerType.MQTT:
            self.__tasks.append(asyncio.create_task(self.__mqtt_connect_loop()))
        
        # loop = asyncio.get_event_loop()