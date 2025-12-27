class MissingDescriptionException(Exception):
    def __init__(self):
        super().__init__("Description is missing")

class MissingVersionException(Exception):
    def __init__(self):
        super().__init__("Version is missing")

class ObjectNotSerializable(Exception):
    def __init__(self, obj):
        super().__init__(f"Could not serialize object of type {type(obj)}")

class ToolValidationException(Exception):
    pass

class ToolRuntimeException(Exception):
    def __init__(self, message: str):
        super().__init__(f"There was an exception while running this tool: {message}")

class CouldNotParseToolRequestException(ToolValidationException):
    def __init__(self, reason: str):
        super().__init__(f"Tool run request invalid: {reason}")

class ToolNotFoundException(ToolValidationException):
    def __init__(self):
        super().__init__("Requested tool not found.")

class InvalidToolParamsException(ToolValidationException):
    def __init__(self, message: str = None):
        super().__init__("Invalid parameters: : " if message is None else f"Invalid parameters: : [{message}]")

class MissingRequiredToolParamsException(InvalidToolParamsException):
    def __init__(self, missing_params: set[str]):
        super().__init__("Missing: " + (",".join(missing_params)))

class TooManyParamsException(InvalidToolParamsException):
    def __init__(self, params_not_in_schema: set[str]):
        super().__init__("paramaters_not_in_schema: " + (",".join(params_not_in_schema)))

class WrongParamTypeException(InvalidToolParamsException):
    def __init__(self, wrong_type_params: set[str]):
        self.response = { "wrong_type_params": wrong_type_params }
        super().__init__(",".join(wrong_type_params))