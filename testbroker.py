import asyncio
import logging

import asyncio
import logging
from uuid import uuid4
from aiomqtt import Client as MQTTClient, MqttError
from dataclasses import dataclass
from enum import Enum
import re
from urllib.parse import unquote
from typing import Callable, Awaitable

from server.broker import BrokerConnectionString, MessagingBroker

logging.basicConfig(level=logging.INFO)

async def main():
    conn = "mqtt://admin:admin@192.168.1.2:1883"
    broker = MessagingBroker(BrokerConnectionString(conn))

    async def on_msg(topic, payload):
        print(f"{topic}: {payload.decode('utf-8')}")

    broker.on_message_cb = on_msg
    await broker.connect()
    await broker.subscribe("/test")

    # Keep alive
    await asyncio.Event().wait()

asyncio.run(main())
