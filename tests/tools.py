from server import decorators
from datetime import datetime
from zoneinfo import ZoneInfo

@decorators.version("21_12_2025-dev-1234-testing")
class System:
    """
    Does stuff with the system
    """

    @decorators.generate_method_schema
    @staticmethod
    def current_datetime(timezone:str="UTC") -> str:
        """
        Returns the current date and time for a specified timezone.

        Args:
            timezone (str): Time zone name, default is UTC
            
        Returns:
            result is current date/time as timestamp string
        """        
        return datetime.now(ZoneInfo(key=timezone)).isoformat()