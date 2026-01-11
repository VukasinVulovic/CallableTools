import asyncio
import logging
import os
from client.interfaces.AMQP import AMQPClient
from common.exceptions import ToolRuntimeException, ToolValidationException
from common.helpers.connStringParser import BrokerConnectionString
import dotenv

dotenv.load_dotenv(".env")
dotenv.load_dotenv(".env.local", override=True)

logging.basicConfig(level=logging.INFO)

async def Multiploop():
    for i in range(0, 100):
        yield i

async def main():
    async with AMQPClient(BrokerConnectionString(conn_str=os.getenv("BROKER"))) as c:
        await c.tools_discovered_ev.wait()
        
        try:
            res = await c.execute("TestingToolbox", "MathWiz.multiply", { "a": "2", "b": 2 })
            print(f"Tool Result: {res}")
        except (ToolValidationException, ToolRuntimeException) as e:
            print(f"Tool Exception: {e}")

if __name__ == "__main__":
    asyncio.run(main())