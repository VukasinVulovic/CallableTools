from enum import Enum
from hashlib import md5
import logging
from typing import AsyncGenerator, Callable
from uuid import uuid4
from server.common.exceptions import CouldNotParseToolRequestException, ToolNotFoundException, ToolRuntimeException, ToolValidationException
from server.schema import Version
from server.tooling import ToolBox
import asyncio
from fastapi import FastAPI
from functools import cache
from server.models import RunToolRequest, ToolResponse, ToolStatus
from server.broker import BrokerConnectionString, MessagingBroker

class EndPoints(Enum):
    RUN_TOOL = "execute"
    TOOL_RESPONSES = "responses"

class HostToolbox:
    __version: Version
    __tool_box: ToolBox
    __server_tasks: list = []
    __broker: MessagingBroker = None
    __logger: logging.Logger

    def __init__(self, tool_box: ToolBox, service_version: Version):
        self.__app = FastAPI()
        self.__broker = None
        self.__tool_box = tool_box
        self.__version = service_version
        self.__logger = logging.getLogger(f"Toolbox-{tool_box.name}")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for t in self.__server_tasks:
            if not t.done():
                t.cancel()
                
        del self.__server_tasks

    #region tool stuff
    async def __use_tool(self, req: RunToolRequest, func=tuple[Callable, dict]) -> ToolResponse: 
        self.__logger.info("NEW Request to run %s.%s", req.tool_box_name, req.tool_name)
               
        try:
            out = await self.__tool_box.execute(func)
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

    async def __tools_names(self) -> AsyncGenerator:
        for tool in self.__tool_box.tools:
            yield tool

    async def __subscribe_tools(self):
        async for tool in self.__tools_names():
            await self.__broker.subscribe(topic=f"{EndPoints.RUN_TOOL.value}/{self.__tool_box.name}/{tool}", qos=2)
            await self.__broker.create_wild_queue(f"{EndPoints.TOOL_RESPONSES.value}/{self.__tool_box.name}/{tool}/+")

    async def __send_tool_response(self, res: ToolResponse):
        topic = f"{EndPoints.TOOL_RESPONSES.value}/{res.tool_box_name}/{res.tool_name}/{res.run_id}"
        self.__logger.info("Responding to request for %s.%s with status: %s", res.tool_box_name, res.tool_name, res.status.name)
        
        payload = res.model_dump_json().encode("utf-8")
        await self.__broker.publish(topic=topic, payload=payload,qos=2,retain=True)

    async def __message_handler(self, topic: str, msg: bytes):
        validation_errors = []
        
        topic = topic.replace("$share.", "")
        topic_split = topic.split(".")
        
        try:
            try:
                req = RunToolRequest.try_parse(msg)
                req.tool_box_name = self.__tool_box.name
                req.tool_name = ".".join(topic_split[2:])
                
                (func, params) = self.__tool_box.deserialize_request(req)
                
                asyncio \
                    .create_task(self.__use_tool(req=req, func=(func, params)))\
                    .add_done_callback(self.__tool_task_cb)
                
                await self.__send_tool_response(ToolResponse.build(req=req, status=ToolStatus.ACCEPTED, success=True, output="Run request accepted"))
            except CouldNotParseToolRequestException as e:
                self.__logger.error(e)    
            except ToolNotFoundException as e:
                await self.__send_tool_response(ToolResponse.build(req=req, status=ToolStatus.REJECTED, output=str(e)))
        except *ToolValidationException as e:
            if type(e) is ExceptionGroup:
                for ex in e.exceptions:
                    validation_errors.append(str(ex))  
            else:
                validation_errors.append(str(e))

        if len(validation_errors) > 0:
            await self.__send_tool_response(ToolResponse.build(req=req, status=ToolStatus.REJECTED, output=",".join(validation_errors)))

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
            
        self.__broker = MessagingBroker(messaging_broker, client_id=client_id)
            
        self.__broker.on_message_cb = self.__message_handler
        
        self.__server_tasks.append(asyncio.create_task(self.__subscribe_tools()))
        self.__server_tasks.append(asyncio.create_task(self.__broker.connect()))
        
        await asyncio.gather(*self.__server_tasks)