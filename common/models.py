from __future__ import annotations
from dataclasses import dataclass
from pydantic import BaseModel
from common.exceptions import CouldNotParseToolRequestException
from enum import Enum
from typing import Optional
import uuid

from server.schema import ToolBox

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
    request_id: uuid.UUID
    tool_box_name: str
    tool_name: str
    status: ToolStatus
    success: Optional[bool] = False
    output: str | None
    
    @staticmethod
    def build(req: RunToolRequest, status: ToolStatus, output: str, success: bool = False) -> ToolResponse:
        return ToolResponse(
            request_id = req.request_id,
            tool_box_name = req.tool_box_name,
            tool_name = req.tool_name,
            status = status,
            success = False if status == ToolStatus.REJECTED else success,
            output = output
        )
        
@dataclass(frozen=True)
class DiscoveryRequest(BaseModel):
    request_id: uuid.UUID
        
@dataclass(frozen=True)
class DiscoveryResponse(BaseModel):
    execute_schema: str
    response_schema: str
    interface: str
    tool_box_schema: dict