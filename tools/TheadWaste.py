from server import decorators
import sys
import time

sys.set_int_max_str_digits(999999)


@decorators.version("21_12_2025-dev-1234-testing")
class ThreadWaste:
    """
    Testing
    """

    @decorators.generate_method_schema
    @staticmethod
    def fact(n: int) -> int:
        """
        Wastes CPU power
        """
        f = 1
        x = []
        
        for i in range(1, n+1):
            f *= n-i
            x.append(f)
            
        return f