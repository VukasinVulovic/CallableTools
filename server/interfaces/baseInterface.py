import abc

from ..tooling import ToolBox

class ToolboxInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, tb: ToolBox): ...
    
    @abc.abstractmethod
    async def open(self) -> None: ...
    
    @abc.abstractmethod
    async def close(self) -> None: ...

    @property
    @abc.abstractmethod
    def get_toolbox(self) -> ToolBox: ...