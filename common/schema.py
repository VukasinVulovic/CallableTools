from __future__ import annotations
from dataclasses import dataclass
import json
from datetime import date, datetime
from hashlib import md5
from typing import Optional
from pydantic import BaseModel, field_validator
from pydantic_core import PydanticCustomError, core_schema
from pydantic.dataclasses import dataclass as pydantic_dataclass
        
@pydantic_dataclass
class Version:
    date: date
    isProduction: bool
    id: int
    tag: Optional[str] = None

    def __str__(self) -> str:
        return (
            f"{self.date.strftime('%d_%m_%Y')}-"
            f"{'prod' if self.isProduction else 'dev'}-"
            f"{self.id}"
            f"{'-' + self.tag.replace('-', '_') if self.tag is not None else ''}"
        )

    @staticmethod
    def parse(version_str: str) -> "Version":
        parts = version_str.split("-")

        if len(parts) not in (3, 4):
            raise ValueError("Invalid version string")

        return Version(
            date=datetime.strptime(parts[0], "%d_%m_%Y").date(),
            isProduction=parts[1] == "prod",
            id=int(parts[2]),
            tag=parts[3] if len(parts) == 4 else None,
        )

    @classmethod
    def __get_pydantic_core_schema__(cls, source_schema, handler):
        base_schema = handler(source_schema)

        def parse_string(v):
            if not isinstance(v, str):
                raise PydanticCustomError("version.type_error", "Expected string")
            try:
                return cls.parse(v)
            except Exception as e:
                raise PydanticCustomError("version.parse_error", str(e))

        string_schema = core_schema.no_info_plain_validator_function(parse_string)

        return core_schema.union_schema([string_schema, base_schema])
        
@dataclass
class Tool():
    name: str
    description: str
    parameter_schema: dict
    required_parameters: list[str]
    output_schema: str
    version: Version
    callable_path: str | None = None

    def __str__(self) -> str:
        return f"""
        {{
            "name": "{self.name}",
            "description": "{self.description.replace("\n", "\\n")}",
            "version": "{self.version}",
            "callable_path": "{self.callable_path}",
            "parameter_schema": {json.dumps(self.parameter_schema)},
            "required_parameters": {json.dumps(self.required_parameters)},
            "output_schema": {json.dumps(self.output_schema)}
        }}
        """

@dataclass
class ToolGroup:
    description: str
    tools: list[Tool]

    def __str__(self) -> str:
        return f"""
        {{
            "description": "{self.description.replace("\n", "\\n")}",
            "tools": [{",".join([str(tool) for tool in self.tools])}]
        }}
        """

@dataclass
class ToolBox:
    name: str
    description: str
    version: Version | None
    tool_groups: list[ToolGroup]

    def __str__(self) -> str:
        return f"""
        {{
            "name": "{self.name}",
            "description": "{self.description}",
            "version": "{self.version if self.version is not None else ""}",
            "tool_groups": [{",".join([str(tg) for tg in self.tool_groups])}]
        }}
        """
        
    def signature_hash(self) -> str: 
        return md5(str(self).encode()).digest().hex()
    
    def __hash__(self):
        return hash(self.signature_hash())