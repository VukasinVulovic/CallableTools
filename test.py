import asyncio
import logging
import os

from server.host import HostToolbox, BrokerConnectionString
from server.schema import Version
from server.tooling import ToolBox
from dotenv import load_dotenv
import tools.MathWiz, tools.TheadWaste

load_dotenv(".env")
load_dotenv(".env.local", override=True)

VERSION="21_12_2025-dev-1234-testing"
TOOLBOX_NAME = "TestingToolbox"

BROKER_PARAMS = BrokerConnectionString(os.getenv("BROKER"))

logging.basicConfig(level=logging.INFO)

async def main():
    tb = ToolBox(TOOLBOX_NAME, "This toolbox is used for testing", [tools.MathWiz, tools.TheadWaste], Version.parse(VERSION))
    
    ev = asyncio.Event()
    
    i = HostToolbox(tb, Version.parse(VERSION))

    await i.host_at(messaging_broker=BROKER_PARAMS)
    
    await ev.wait()

if __name__ == "__main__":
    asyncio.run(main())