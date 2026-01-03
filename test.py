import asyncio
import logging
import os

from server.schema import Version
from server.tooling import ToolBox
from dotenv import load_dotenv
import tools.MathWiz, tools.TheadWaste

load_dotenv(".env")
load_dotenv(".env.local", override=True)

VERSION="21_12_2025-dev-1234-testing"
TOOLBOX_NAME = "TestingToolbox"

# logging.basicConfig(level=logging.INFO)

logging.basicConfig(level=logging.CRITICAL)

async def main():
    tb = ToolBox(TOOLBOX_NAME, "This toolbox is used for testing", [tools.MathWiz, tools.TheadWaste], Version.parse(VERSION))
    
    stream = tb.handle_raw_request("MathWiz.multiply", """
        {
            "request_id": "00000000-0000-0000-0000-000000000000",
            "parameters": {
                "a": 2,
                "b": "3"
            }
        }
    """.encode("utf-8"))
    
    async for res in stream:
        print(f"{res.status.name}(success={res.success}): {res.output} -> {res.run_id}")
    
    # ev = asyncio.Event()
    
    
    # await ev.wait()

if __name__ == "__main__":
    asyncio.run(main())