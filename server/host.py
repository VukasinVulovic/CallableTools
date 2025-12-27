from enum import Enum
from hashlib import md5
import json
import logging
import sys
import time
from urllib.parse import unquote

from dataclasses_json import dataclass_json
from fastapi.responses import JSONResponse
from server.exceptions import CouldNotParseToolRequestException, ToolNotFoundException, ToolRuntimeException, ToolValidationException
from server.tooling import ToolBox
import re
import asyncio
import uvicorn
from fastapi import FastAPI, Request
from aiomqtt import Client as MQTTClient, MqttError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_404_NOT_FOUND
from dataclasses import dataclass
from functools import cache

from server.schema import RunToolRequest, ToolResponse, ToolStatus, Version
from server.helpers.schema import SchemaSerializer


if sys.platform.startswith("win"): #Windows bs :D
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@dataclass_json
@dataclass
class ErrorResponseType:
    errors: list[str]

@dataclass_json
@dataclass
class ServicesType:
    brokers: list[str]
    offer_endpoint: list[str]

@dataclass
class BrokerParams:
    host: str
    port: int
    username: str | None
    password: str | None

    def __init__(self, conn_str: str):
        m = re \
            .compile(
                r'^mqtt:\/\/(?:(?P<user>[^:\/@]+)(?::(?P<password>[^@\/]+))?@)?'
                r'(?P<host>[^:\/]+)(?::(?P<port>\d+))?$'
            ) \
            .match(conn_str) \
            .groupdict().items()
        
        parsed = { k: unquote(v) if v else None for k, v in m }

        self.host = parsed["host"]
        self.port = int(parsed.get("port", 1883))
        self.username = parsed.get("user")
        self.password = parsed.get("password")

class _ErrorHandlingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)

            match response.status_code:
                case 404:
                    return JSONResponse(
                        status_code=HTTP_404_NOT_FOUND,
                        content=ErrorResponseType(["Endpoint not found"]).__dict__
                    )
        
                case _:
                    return response

        except StarletteHTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content=ErrorResponseType([exc.detail]).__dict__
            )
        
        except Exception as exc:
            logging.exception(exc)
            
            return JSONResponse(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                content=ErrorResponseType(["Server error"]).__dict__
            )

class EndPoints(Enum):
    ROOT = "/"
    OFFERS = "/offers"
    RUN_TOOL = "$share/execute"
    TOOL_RESPONSES = "responses"

class HostToolboxes:
    __version: Version
    __tool_boxes: dict[str, ToolBox]
    __app: FastAPI
    __uvicorn_server: uvicorn.Server
    __server_tasks: list

    def __init__(self, tool_boxes: set[ToolBox], service_version: Version):
        self.__app = FastAPI()
        self.__broker = None
        self.__tool_boxes = { box.__schema__.name: box for box in tool_boxes}
        self.__version = service_version

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for t in self.__server_tasks:
            if not t.done():
                t.cancel()
                
        del self.__server_tasks

    async def __start_fastapi(self, at):
        self.__uvicorn_server = uvicorn.Server(uvicorn.Config(
            app=self.__app,
            host=at[0],
            port=at[1],
            loop="asyncio"
        ))

        await self.__uvicorn_server.serve()

    async def __broker_subscribe_tools(self):
        for name, tb in self.__tool_boxes.items():
            for tool in tb.tools:
                topic = f"{EndPoints.RUN_TOOL.value}/{name}/{tool}"
                await self.__broker.subscribe(topic=topic, qos=2)
                logging.info(f"Subscribed to {topic}")

    async def __broker_send_tool_response(self, res: ToolResponse):
        topic = f"{EndPoints.TOOL_RESPONSES.value}/{res.run_id}"
        logging.info(f"Responding to tool request @ {topic}, {res.status.name}")
        
        payload = res.model_dump_json().encode("utf-8")
        return asyncio.create_task(self.__broker.publish(topic=topic, payload=payload,qos=2,retain=True))

    async def __use_tool(self, toolbox: ToolBox, req: RunToolRequest) -> ToolResponse:
        errors = []
        
        try:
            try:
                out = await toolbox.use_tool(req.tool_name, req.parameters)
            except* ToolValidationException as e:
                if type(e) is ExceptionGroup:
                    for ex in e.exceptions:
                        errors.append(str(ex))  
                else:
                    errors.append(str(e))              
        except ToolRuntimeException as e:
            if type(e) is ExceptionGroup:
                for ex in e.exceptions:
                    errors.append(str(ex))  
            else:
                errors.append(str(e))    
        except Exception as e:
            logging.exception(e)
            return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output="Internal server error", success=False)
            
        #TODO: serialize complex types
        
        if len(errors) > 0:
            return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=",".join(errors), success=False)
        
        return ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=str(out), success=True)
        
    def __tool_task_cb(self, task: asyncio.Task):
        if task.done():
            asyncio.create_task(self.__broker_send_tool_response(task.result()))

    async def __broker_message_handler(self):
        if not self.__broker._client.is_connected():
            raise MqttError("Not connected")
        
        await self.__broker_subscribe_tools()
    
        async with self.__broker.messages() as queue:
            async for msg in queue:
                logging.info(f"New Broker message @ {msg.topic}")
                #TODO: Validate path
                topic_split = msg.topic.value.split("/")
                
                try:
                    req = RunToolRequest.try_parse(msg.payload.decode("utf-8"))
                    req.tool_box_name = topic_split[-3]
                    req.tool_name = ".".join(topic_split[-2:])
                    tb = self.__tool_boxes.get(req.tool_box_name, None)
                    
                    if tb is None:
                        raise ToolNotFoundException()
                    
                    asyncio \
                        .create_task(self.__use_tool(tb, req))\
                        .add_done_callback(self.__tool_task_cb)
                except CouldNotParseToolRequestException as e:
                    logging.error(e)
                
                except (ToolValidationException, ToolNotFoundException) as e:
                    asyncio.create_task(self.__broker_send_tool_response(ToolResponse.build(req=req, status=ToolStatus.REJECTED, output=str(e))))

    async def __run_broker(self, broker: BrokerParams, server_host: str):    
        while True: #auto reconnect loop
            try:
                if self.__broker:
                    try:
                        await self.__broker.disconnect()
                    except Exception as e:
                        logging.warning(f"Broker disconnect error {e}, retrying...")
                
                client_id = "ToolboxRunner_" \
                    + md5(
                        (
                            "".join(self.__tool_boxes.keys()) \
                                + __file__ \
                                + server_host[0] \
                                + str(server_host[1])
                        ).encode("utf-8")
                    ).hexdigest()
                
                self.__broker = MQTTClient(
                    broker.host, 
                    broker.port, 
                    clean_session=False,
                    client_id=client_id, 
                    password=broker.password, 
                    username=broker.username
                )
                
                await self.__broker.connect()
                
                logging.info(f"Connected to broker as: {client_id}")
                try:
                    await self.__broker_message_handler()
                except Exception as e:
                    logging.exception(e)
            except MqttError as e:
                logging.exception(e)
                time.sleep(3)

    #region Endpoint methods
    # TODO: Expand
    @cache
    def __root_endpoint(self) -> dict:
        return {
            "name": "toolbox-api",
            "service_version": str(self.__version),
            "endpoints": {
                "root": EndPoints.ROOT.value,
                "tool_boxes": EndPoints.OFFERS.value
            }
            # ,"brokers": [f"{self.__broker._hostname}:{self.__broker._port}"]  if self.__broker else []
        }
    
    # TODO: Expand
    @cache
    def __offers_endpoint(self) -> dict:
        return { "toolboxes": json.loads(SchemaSerializer.serialize(list(self.__tool_boxes.values()))) }

    def __add_main_routes(self):
        #offer of toolboxes
        self.__app.add_middleware(_ErrorHandlingMiddleware)
        self.__app.add_api_route(EndPoints.ROOT.value, self.__root_endpoint, methods=["GET"])
        self.__app.add_api_route(EndPoints.OFFERS.value, self.__offers_endpoint, methods=["GET"])

    #endregion Endpoint methods

    async def serve(self, at: tuple[str, int]=("127.0.0.1", 80), messaging_broker: BrokerParams | None = None) -> None:
        """
        Runs the api server for selected toolboxes.
        
        :param at: listening location as touple of (host, port), defaults to ("127.0.0.1", 80)
        :type at: tuple[str, int]
        :param messaging_broker: Connection string of Messaging broker (MQTT or AMPQ) for async requests, if default left (None), the server will function via socket.io
        :type messaging_broker: str | None
        """

        self.__server_tasks = []
        
        if messaging_broker:
            self.__server_tasks.append(asyncio.create_task(self.__run_broker(messaging_broker, at)))

        self.__add_main_routes()

        self.__server_tasks.append(asyncio.create_task(self.__start_fastapi(at)))
        
        await asyncio.gather(*self.__server_tasks)