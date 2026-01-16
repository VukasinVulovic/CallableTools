import asyncio
import logging
import os

from server.interfaces.MessagingBroker import AMQPInterface
from common.schema import Version
from server.tooling import ToolBox
from dotenv import load_dotenv
from common.helpers.connStringParser import BrokerConnectionString

from . import tools

logging.basicConfig(level=logging.INFO)

load_dotenv(".env")
load_dotenv(".env.local", override=True)

VERSION="21_12_2025-dev-1234-testing"
TOOLBOX_NAME = "TestingToolbox"

async def main():
    ev = asyncio.Event()    
    tb = ToolBox(TOOLBOX_NAME, "This toolbox is used for testing", [tools], Version.parse(VERSION))
    
    async with AMQPInterface(tb, BrokerConnectionString(conn_str=os.getenv("BROKER"))):
        await ev.wait()
    

if __name__ == "__main__":
    asyncio.run(main())