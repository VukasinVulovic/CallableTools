import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from server import schema
from server.decorators import generate_method_schema
from server.common.exceptions import *
import types as pyTypes
import inspect

from server.helpers.schema import SchemaParser

import tracemalloc
tracemalloc.start()

class ToolBox:
    __schema__: schema.ToolBox = None
    __tools: dict[str, callable]
    _executor: ThreadPoolExecutor

    def __init__(self, name: str, description: str, modules: list[pyTypes.ModuleType], version: schema.Version = None):
        tool_groups = list[schema.ToolGroup]()
        self._executor = ThreadPoolExecutor()
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

    # @cache
    async def use_tool(self, tool_name=str, params=dict) -> Any:
        func = self.__tools.get(tool_name or "\\")

        if func is None:
            raise ToolNotFoundException()
        
        params = SchemaParser.try_parse_params(func.__schema__, params)

        try:
            if asyncio.iscoroutinefunction(func):
                # func is async
                outputs = await func(**params)
            else:
                # func is blocking -> run in thread pool
                loop = asyncio.get_running_loop()
                outputs = await loop.run_in_executor(self._executor, lambda: func(**params))
        except Exception as e:
            raise ToolRuntimeException(str(e)) #no trace
        
        return outputs
    
    @property
    def tools(self):
        return self.__tools