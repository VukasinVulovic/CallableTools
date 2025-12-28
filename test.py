import asyncio
import logging
import os

from server.host import HostToolboxes, BrokerConnectionString
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

if __name__ == "__main__":
    tb = ToolBox(TOOLBOX_NAME, "This toolbox is used for testing", [tools.MathWiz, tools.TheadWaste], Version.parse(VERSION))
    
    i = HostToolboxes([tb], Version.parse(VERSION))

    asyncio.run(i.serve(messaging_broker=BROKER_PARAMS, at=("127.0.0.1", 12)))