from enum import Enum

class _AMQPRoutes(Enum):
    DISCOVERY = "discover"
    EXECUTE = "execute"
    UPDATES = "tools.update"