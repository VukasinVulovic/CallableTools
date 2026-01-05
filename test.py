import asyncio
import logging
import os

from server.interfaces.MessagingBroker import MQTTInterface
from server.schema import Version
from server.tooling import ToolBox
from dotenv import load_dotenv
import tools.MathWiz, tools.TheadWaste
from server.helpers.connStringParser import BrokerConnectionString

load_dotenv(".env")
load_dotenv(".env.local", override=True)

VERSION="21_12_2025-dev-1234-testing"
TOOLBOX_NAME = "TestingToolbox"

logging.basicConfig(level=logging.INFO)

async def main():
    ev = asyncio.Event()    
    tb = ToolBox(TOOLBOX_NAME, "This toolbox is used for testing", [tools.MathWiz, tools.TheadWaste], Version.parse(VERSION))
    
    async with MQTTInterface(tb, BrokerConnectionString(conn_str=os.getenv("BROKER"))) as iface:
        await ev.wait()
    
    
    # stream = tb.handle_raw_request("MathWiz.multiply", """
    #     {
    #         "request_id": "00000000-0000-0000-0000-000000000000",
    #         "parameters": {
    #             "a": 2,
    #             "b": "3"
    #         }
    #     }
    # """.encode("utf-8"))
    
    # async for res in stream:
    #     print(f"{res.status.name}(success={res.success}): {res.output} -> {res.run_id}")
    
    
    

if __name__ == "__main__":
    asyncio.run(main())