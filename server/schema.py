from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
import json
from datetime import date, datetime
from typing import Optional
import uuid

from pydantic import BaseModel

from server.exceptions import CouldNotParseToolRequestException

@dataclass
class Version:
    date: date
    isProduction: bool
    id: int
    tag: str | None = None

    def __str__(self) -> str:
        return f"{self.date.strftime("%d_%m_%Y")}-{"prod" if self.isProduction else "dev"}-{self.id}{"-"+self.tag.replace("-", "_") if self.tag is not None else ""}"

    def parse(version_str: str) -> Version:
        parts = version_str.split("-")
        
        return Version(
            date=datetime.strptime(parts[0], "%d_%m_%Y").date(),
            isProduction=parts[1] == "prod",
            id=int(parts[2]),
            tag=parts[3]       
        )

@dataclass
class Tool:
    name: str
    description: str
    parameter_schema: dict
    required_parameters: list[str]
    output_schema: dict
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
    
class RunToolRequest(BaseModel):
    tool_box_name: Optional[str] = None
    tool_name: Optional[str] = None
    parameters: Optional[dict] = {}
    stop_on_fail: Optional[bool] = False
    request_id: uuid.UUID
    
    def try_parse(str: str) -> RunToolRequest:
        try:
            return RunToolRequest.model_validate_json(str)
        except Exception as e:
            raise CouldNotParseToolRequestException(e)

class ToolStatus(Enum):
    REJECTED = "REJECT"
    ACCEPTED = "ACCEPT"
    PROCESSED = "DONE"

@dataclass(frozen=True)
class ToolResponse(BaseModel):
    run_id: uuid.UUID
    tool_box_name: str
    tool_name: str
    status: ToolStatus
    success: Optional[bool] = False
    output: str | None
    
    @staticmethod
    def build(req: RunToolRequest, status: ToolStatus, output: str, success: bool = False) -> ToolResponse:
        return ToolResponse(
            run_id = req.request_id,
            tool_box_name = req.tool_box_name,
            tool_name = req.tool_name,
            status = status,
            success = False if status == ToolStatus.REJECTED else success,
            output = output
        )