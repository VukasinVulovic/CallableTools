import inspect
from ..common import schema
from typing import Callable, Any, TypeVar, Union
from CallableTools.common.helpers.schema import gen_type_schema
from CallableTools.common.exceptions import MissingDescriptionException

DECORATABLE_TYPE = TypeVar("T", bound=Union[type, Callable[..., Any]])

def generate_method_schema(func: Callable):
    """
    Decorator for a function that generates schema for that function.
    :param func: function
    """""

    f = func.__func__ if hasattr(func, "__func__") else func

    # Get signature without binding artifacts
    sig = inspect.signature(f)

    ver = getattr(f, "__version__", None)

    if f.__doc__ is None or not f.__doc__.strip():
        raise MissingDescriptionException()

    # if ver is None:
    #     raise MissingVersionException()

    f.__schema__ = \
        schema.Tool( \
            name=f.__qualname__, \
            description=f.__doc__.strip("\n").strip(" "), \
            parameter_schema=dict([(item[0], gen_type_schema(item[1])) for item in dict(filter(lambda a: a[0] != "return", f.__annotations__.items())).items()]), \
            required_parameters=[name for name, param in sig.parameters.items() if param.default is inspect.Parameter.empty], \
            output_schema=gen_type_schema(f.__annotations__.get('return')) if f.__annotations__.get('return') is not None else None, \
            callable_path="???",
            version=ver
        )

    return func

def version(version_str: str):
    def decorator(obj: DECORATABLE_TYPE) -> DECORATABLE_TYPE:
        ver = schema.Version.parse(version_str)

        obj.__version__ = ver

        #set fallback versions for all methods
        if isinstance(obj, type):
            for _, attr in obj.__dict__.items():
                if isinstance(attr, staticmethod):
                    attr.__func__.__version__ = ver

        return obj
    
    return decorator