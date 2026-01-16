import asyncio
import logging
import os
import dotenv

from client.interfaces.AMQP import AMQPClient
from common.exceptions import ToolRuntimeException, ToolValidationException
from common.helpers.connStringParser import BrokerConnectionString

dotenv.load_dotenv(".env")
dotenv.load_dotenv(".env.local", override=True)

logging.basicConfig(level=logging.INFO)

async def Multiploop():
    for i in range(0, 100):
        yield i

async def main():
    ev = asyncio.Event()
    
    async with AMQPClient(BrokerConnectionString(conn_str=os.getenv("BROKER"))) as c:
        await c.tools_discovered_ev.wait()
                                    
        try:
            res = await c.execute("TestingToolbox", "System.request_test", { "url": "https://google.com" })
            print(f"Tool Result: {res}")
        except (ToolValidationException, ToolRuntimeException) as e:
            print(f"Tool Exception: {e}")

if __name__ == "__main__":
    asyncio.run(main())