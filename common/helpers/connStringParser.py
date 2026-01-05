from dataclasses import dataclass
from enum import Enum
import re
from urllib.parse import unquote


class BrokerType(Enum):
    MQTT = "mqtt"
    AMQP = "amqp"
    AMQPS = "amqps"
    
@dataclass
class BrokerConnectionString:
    host: str
    port: int
    type: BrokerType
    username: str | None
    password: str | None

    def __init__(self, conn_str: str):
        m = re \
            .compile(
                r'^(?P<scheme>mqtt|amqp[s]?)://'
                r'(?:(?P<user>[^:/@]+)(?::(?P<password>[^@/]+))?@)?'
                r'(?P<host>[^:/]+)'
                r'(?::(?P<port>\d+))?'
                r'(?:/(?P<path>[^/]+))?$'
            ) \
            .match(conn_str) \
            .groupdict() \
            .items()
        
        parsed = { k: unquote(v) if v else None for k, v in m }

        self.type = BrokerType(parsed.get("scheme"))
        self.host = parsed.get("host")
        self.port = int(parsed.get("port", 1883))
        self.username = parsed.get("user")
        self.password = parsed.get("password")