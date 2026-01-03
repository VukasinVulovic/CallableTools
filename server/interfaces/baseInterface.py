import abc
from typing import Awaitable, Callable

from server.models import RunToolRequest, ToolResponse
from ..tooling import ToolBox

class ToolboxInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def open(self) -> None: ...
    
    @abc.abstractmethod
    async def close(self) -> None: ...
    
    @abc.abstractmethod
    async def serve(self, tb: ToolBox) -> None: ...
    
    @property
    @abc.abstractmethod
    def on_request_cb(self):
        raise AttributeError("write-only")

    @property
    @abc.abstractmethod
    def get_toolbox(self) -> ToolBox: ...

    @on_request_cb.setter
    @abc.abstractmethod
    def on_request_cb(self, cb: Callable[[RunToolRequest], Awaitable[None]]) -> None: ...
    
    @abc.abstractmethod
    async def respond_to_request(self, res: ToolResponse) -> None: ...