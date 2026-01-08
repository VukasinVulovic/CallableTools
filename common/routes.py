from enum import Enum


class _MQTTTopics(Enum):
    DISCOVERY = "$share/discover"
    EXECUTE = "$share/execute"
    RESPONSE = "response"

class _AMQPExchanges(Enum):
    DISCOVERY = "discover"
    EXECUTE = "execute"