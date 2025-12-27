from server import decorators


@decorators.version("21_12_2025-dev-1234-testing")
class MathWiz:
    """
    Mathematical functions for Arithmetics and other math things
    """

    @decorators.generate_method_schema
    @staticmethod
    def multiply(a: int, b: int) -> int:
        """
        Multiplies two numbers together.
        """

        return a * b