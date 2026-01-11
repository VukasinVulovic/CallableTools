import asyncio
import importlib
import inspect
import logging
import os
from pathlib import Path
import sys
import time
import types as pyTypes
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any, AsyncGenerator, Callable, Coroutine

from common.exceptions import CouldNotParseToolRequestException, MissingDescriptionException, MissingVersionException, ToolNotFoundException, ToolRuntimeException, ToolValidationException
from common.helpers.schema import SchemaParser
from common.models import RunToolRequest, ToolResponse, ToolStatus
from common import schema
from server.decorators import generate_method_schema

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileModifiedEvent

def _reload_module_from_path(path: str):
    path = Path(path).resolve()
    module_name = os.path.splitext(os.path.basename(path))[0]

    importlib.reload(sys.modules[module_name])

#TODO: add validate_tool
class ToolBox:
    __name: str = None
    __version: schema.Version = None
    __description: str = None
    __modules: list[pyTypes.ModuleType] = []
    
    __schema__: schema.ToolBox = None
    __tools: dict[str, callable] = dict()
    __executor: ThreadPoolExecutor = ThreadPoolExecutor()
    __logger: Logger = logging.Logger
    _on_change_cb: Callable
    __file_observer =  Observer()
    
    UPDATE_DEBOUNCE_DELAY=0.3
    
    def __init__(self, name: str, description: str, modules: list[pyTypes.ModuleType], version: schema.Version = None):
        self.__name = name
        self.__description = description
        self.__modules = modules
        self.__version = version
        self.__logger = logging.getLogger(f"TB-{name}")
        self.__observe_updates()
        self.__load_schema()
        self.__loop = asyncio.get_event_loop()
        self.__last_file_change = 0

    def __on_file_change(self, e: FileModifiedEvent):
        if not self._on_change_cb:
            return
        
        now = time.monotonic()
        
        if now - self.__last_file_change < self.UPDATE_DEBOUNCE_DELAY:
            return  # debounce
        
        self.__last_file_change = now
        
        _reload_module_from_path(e.src_path)
        
        self.__load_schema()
        asyncio.run_coroutine_threadsafe(self._on_change_cb(), self.__loop)
        
    def __observe_updates(self):
        handler = PatternMatchingEventHandler(
            patterns=list(map(lambda m: m.__file__, self.__modules)),
            ignore_directories=True
        )
        
        handler.on_modified = handler.on_created = handler.on_moved = self.__on_file_change
        
        for dir in list(map(lambda m: str(Path(m.__file__).resolve().parent), self.__modules)):
            self.__file_observer.schedule(path=dir, recursive=False, event_handler=handler)
        
        self.__file_observer.start()

    def __load_schema(self):
        tool_groups = list[schema.ToolGroup]()

        for mod in self.__modules:
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
                        
                        func.__schema__.callable_path = f"{self.__name}.{func.__schema__.name}"

                        tools.append(func.__schema__)
                        self.__tools[func.__schema__.name] = func

                if len(tools) > 0:
                    if cls.__doc__ is None:
                        raise MissingDescriptionException()

                    tool_groups.append(schema.ToolGroup(cls.__doc__.strip("\n").strip(" "), tools))

        self.__schema__ = schema.ToolBox(
            name=self.__name,
            description=self.__description,
            version=self.__version,
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
    def on_change_cb(self):
        raise Exception("Get not allowed for this property!")
    
    @on_change_cb.setter
    def on_change_cb(self, cb):
        self._on_change_cb = cb
    
    @property
    def tools(self):
        return self.__tools
    
    @property
    def name(self):
        return self.__schema__.name