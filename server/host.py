from enum import Enum
from hashlib import md5
import logging
from typing import AsyncGenerator, Callable
from uuid import uuid4
from server.common.exceptions import CouldNotParseToolRequestException, ToolNotFoundException, ToolRuntimeException, ToolValidationException
from server.interfaces.baseInterface import ToolboxInterface
from server.schema import Version
from server.tooling import ToolBox
import asyncio
from fastapi import FastAPI
from functools import cache
from server.models import RunToolRequest, ToolResponse, ToolStatus

class EndPoints(Enum):
    RUN_TOOL = "execute"
    TOOL_RESPONSES = "responses"

class HostToolbox:
    __tool_box: ToolBox
    __server_tasks: list = []
    __logger: logging.Logger
    __interface: ToolboxInterface

    def __init__(self, tool_box: ToolBox):
        self.__broker = None
        self.__tool_box = tool_box
        self.__logger = logging.getLogger(f"Toolbox-{tool_box.name}")

    #region tool stuff
    async def __use_tool(self, req: RunToolRequest, func=tuple[Callable, dict]) -> ToolResponse: 
        self.__logger.info("NEW Request to run %s.%s", req.tool_box_name, req.tool_name)
               
        try:
            out = await self.__tool_box.__execute_func(func)
        except ToolRuntimeException as e:
            return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=str(e), success=False)

        except Exception as e:
            self.__logger.exception(e)
            return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output="Internal server error", success=False)
            
        #TODO: serialize complex types
        
        return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=str(out), success=True)
        
    def __tool_task_cb(self, task: asyncio.Task):
        if task.done():
            asyncio.create_task(self.__send_tool_response(task.result()))
    #endregion

    async def __send_tool_response(self, res: ToolResponse):
        topic = f"{EndPoints.TOOL_RESPONSES.value}/{res.tool_box_name}/{res.tool_name}/{res.run_id}"
        self.__logger.info("Responding to request for %s.%s with status: %s", res.tool_box_name, res.tool_name, res.status.name)
        
        payload = res.model_dump_json().encode("utf-8")
        await self.__broker.publish(topic=topic, payload=payload,qos=2,retain=True)



    async def host_at(self, messaging_broker: BrokerConnectionString) -> None:
        """
        Runs the api server for selected toolbox.
        :param messaging_broker: Connection string of Messaging broker (MQTT or AMPQ) for async requests, if default left (None), the server will function via socket.io
        :type messaging_broker: str | None
        """

        client_id = f"{self.__tool_box.name}." \
            + md5((__file__ + str(messaging_broker)).encode("utf-8"))\
            .hexdigest() \
            + str(uuid4())
            
        self.__logger.info("Trying to connect to %s @ %s:%s", messaging_broker.type.name, messaging_broker.host, messaging_broker.port)
            
        self.__broker = BrokerHandler(messaging_broker, client_id=client_id)
            
        self.__broker.on_message_cb = self.handle_raw_request
        
        self.__server_tasks.append(asyncio.create_task(self.__subscribe_tools()))
        self.__server_tasks.append(asyncio.create_task(self.__broker.connect()))
        
        await asyncio.gather(*self.__server_tasks)