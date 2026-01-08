import asyncio
import os
from client.AMQP import AMQPClient
from common.exceptions import ToolRuntimeException, ToolValidationException
from common.helpers.connStringParser import BrokerConnectionString
import dotenv

dotenv.load_dotenv(".env")
dotenv.load_dotenv(".env.local", override=True)

async def Multiploop():
    for i in range(0, 100):
        yield i

async def main():
    async with AMQPClient(BrokerConnectionString(conn_str=os.getenv("BROKER"))) as c:
        try:
            async for i in Multiploop():
                res = await c.execute("TestingToolbox", "MathWiz.multiply", { "a": "2", "b": i*1_000_000 })
                print(f"Tool Result: {res}")
        except (ToolValidationException, ToolRuntimeException) as e:
            print(f"Tool Exception: {e}")

if __name__ == "__main__":
    asyncio.run(main())