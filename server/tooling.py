import asyncio
import inspect
import logging
import tracemalloc
import types as pyTypes
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any, AsyncGenerator, Callable, Coroutine

from common.exceptions import CouldNotParseToolRequestException, ToolNotFoundException, ToolRuntimeException, ToolValidationException
from common.helpers.schema import SchemaParser
from common.models import RunToolRequest, ToolResponse, ToolStatus
from server import schema
from server.decorators import generate_method_schema

tracemalloc.start()

#TODO: add validate_tool

class ToolBox:
    __schema__: schema.ToolBox = None
    __tools: dict[str, callable]
    __executor: ThreadPoolExecutor
    __logger: Logger

    def __init__(self, name: str, description: str, modules: list[pyTypes.ModuleType], version: schema.Version = None):
        self.__logger = logging.getLogger(f"TB-{name}")
        
        tool_groups = list[schema.ToolGroup]()
        self.__executor = ThreadPoolExecutor()
        self.__tools = dict()

        for mod in modules:
            for _, cls in inspect.getmembers(mod):
                tools = []

                for _, member in inspect.getmembers(cls):
                    if isinstance(member, (staticmethod, classmethod)):
                        func = member.__func__
                    elif inspect.isfunction(member):
                        func = member
                    else:
                        continue

                    if hasattr(func, "__schema__"):
                        if func.__schema__.version is None:
                            func = generate_method_schema(func)

                        if func.__schema__.version is None:
                            raise MissingVersionException()
                        
                        func.__schema__.callable_path = f"{name.replace(".", "/")}/{func.__schema__.name}"

                        tools.append(func.__schema__)
                        self.__tools[func.__schema__.name] = func

                if len(tools) > 0:
                    if cls.__doc__ is None:
                        raise MissingDescriptionException()

                    tool_groups.append(schema.ToolGroup(cls.__doc__.strip("\n").strip(" "), tools))

        self.__schema__ = schema.ToolBox(
            name=name,
            description=description,
            version=version,
            tool_groups=[str(t) for t in tool_groups]
        )

    async def __execute_func(self, func: Callable | Coroutine, params: dict = {}) -> Any:
        try:
            if asyncio.iscoroutinefunction(func):
                # func is async
                outputs = await func(**params)
            else:
                # func is blocking -> run in thread pool
                loop = asyncio.get_running_loop()
                outputs = await loop.run_in_executor(self.__executor, lambda: func(**params))
        except Exception as e:
            self.__logger.exception(e)
            raise ToolRuntimeException("Internal server error") #no trace
        
        return outputs
    
    async def handle_raw_request(self, tool: str, raw_request: str | bytes) -> AsyncGenerator[ToolResponse, None]:
        self.__logger.info(f"NEW Request for {tool}")
        
        validation_errors = []
        
        try:
            try:
                req = RunToolRequest.try_parse(raw_request if type(raw_request) is str else raw_request.decode())
                req.tool_box_name = self.name
                req.tool_name = tool
                    
                #TODO: serialize complex types                
                func = self.__tools.get(req.tool_name or "\\")

                if func is None:
                    yield ToolNotFoundException()
                
                params =  SchemaParser.try_parse_params(func.__schema__, req.parameters)
                
                yield ToolResponse.build(req=req, status=ToolStatus.ACCEPTED, output="Accepted", success=True)
                self.__logger.info(f"Request for {tool} ACCEPTED")
                                            
                out = await self.__execute_func(func, params)
                
                yield ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=str(out), success=True)
                self.__logger.info(f"Request for {tool} PROCESSED")
            except CouldNotParseToolRequestException as e:
                self.__logger.exception(e)
            except ToolNotFoundException as e:
                yield ToolResponse.build(req=req, status=ToolStatus.REJECTED, output=str(e))
                self.__logger.info(f"Request for {tool} REJECTED")
            except ToolRuntimeException as e:
                yield ToolResponse.build(req=req, status=ToolStatus.PROCESSED, output=str(e), success=False)
                self.__logger.info(f"Request for {tool} PROCESSED, EXCEPTION AT RUNTIME")
        except *ToolValidationException as e:
            if type(e) is ExceptionGroup:
                for ex in e.exceptions:
                    validation_errors.append(str(ex))  
            else:
                validation_errors.append(str(e))

        if len(validation_errors) > 0:
            yield ToolResponse.build(req=req, status=ToolStatus.REJECTED, output=",".join(validation_errors))
            self.__logger.info(f"Request for {tool} REJECTED")
    
    @property
    def tools(self):
        return self.__tools
    
    @property
    def name(self):
        return self.__schema__.name