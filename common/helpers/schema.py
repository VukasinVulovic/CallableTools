import json
from typing import get_type_hints, Any
from ...common.exceptions import MissingRequiredToolParamsException, ObjectNotSerializable, TooManyParamsException, WrongParamTypeException
from ...common.schema import Tool

BASIC_TYPES_STR = {
    str: "string",
    int: "int",
    float: "float",
    bool: "bool"
} 

BASIC_TYPES_PARSED = {
    "string": str,
    "int": int,
    "float": float,
    "bool": bool
} 


def gen_type_schema(obj):
    """
    Convert a Python type or class instance into a JSON-like schema.
    
    Basic types:
        str -> "string"
        int -> "int"
        float -> "float"
        bool -> "boolean"
    
    Classes: recursively inspect properties and build a dictionary schema.
    """
    
    #return basic types
    if obj in BASIC_TYPES_STR:
        return BASIC_TYPES_STR[obj]
    
    origin = getattr(obj, "__origin__", None)
    args = getattr(obj, "__args__", None)
    if origin:
        if origin is list or origin is list:
            return [gen_type_schema(args[0])]
        if origin is dict or origin is dict:
            return {gen_type_schema(args[0]): gen_type_schema(args[1])}
    
    # If cmplex type, generate schema untill simple type is hit
    if isinstance(obj, type):
        schema = {}
        hints = get_type_hints(obj)
        for attr, attr_type in hints.items():
            schema[attr] = gen_type_schema(attr_type)
        return schema
    
    if hasattr(obj, "__dict__"):
        return gen_type_schema(type(obj))
    
    # fallback
    return "string"

class SchemaParser:
    @staticmethod
    def try_parse_params(func_schema: Tool, params: dict):
        if len(func_schema.parameter_schema) == 0:
            if len(params) > 0:
                raise TooManyParamsException(params.keys())
            
            return {}

        missing_params = set(func_schema.required_parameters) - params.keys()
        params_not_in_schema = params.keys() - func_schema.parameter_schema

        errors = []

        #check if required params supplied
        if len(missing_params) > 0:
            errors.append(MissingRequiredToolParamsException(missing_params))

        if len(params_not_in_schema) > 0:
            errors.append(TooManyParamsException(params_not_in_schema))

        output_params = {}
        failed_conversions = set()

        #try to convert to correct types
        for param, val in params.items():
            if param not in func_schema.parameter_schema: #skip params not in schema
                continue

            #already ok type
            if gen_type_schema(param) is func_schema.parameter_schema[param]:
                output_params[param] = val
                continue

            try:
                #for bools, convert: true -> True, false -> False, check if any of those (don't accept 1 as True, 0 as False for ex.)
                if func_schema.parameter_schema[param] == "bool":
                    param = str(param).capitalize()

                    if param not in ["True", "False"]:
                        failed_conversions.add(param)
                        continue

                type_ = BASIC_TYPES_PARSED.get(func_schema.parameter_schema[param])
                output_params[param] = type_(val)
            except (TypeError, ValueError):
                failed_conversions.add(param)

        if len(failed_conversions) > 0:
            errors.append(WrongParamTypeException(failed_conversions))

        if len(errors) > 0:
            raise ExceptionGroup("Parsing exceptions", errors)
        
        return output_params

class SchemaSerializer:
    def serialize(obj: Any) -> str:
        # list / tuple
        if isinstance(obj, (list, tuple)):
            return json.dumps([json.loads(SchemaSerializer.serialize(v)) for v in obj])

        # dict
        if isinstance(obj, dict):
            return json.dumps({str(k): json.loads(SchemaSerializer.serialize(v)) for k, v in obj.items()})
        
        # Use shema
        if hasattr(obj, "__schema__"):
            return str(obj.__schema__)
        
        raise ObjectNotSerializable(obj)