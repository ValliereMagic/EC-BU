def parse_integer_argument(int_arg: str, error_str: str) -> int or None:
    """
    Parse a user entered integer argument as an integer from a string.
    If an error occurs, inform the user.
    """
    resultant_int: int = None
    if int_arg:
        try:
            resultant_int = int(int_arg)
            # Make sure the argument isn't out of sensible bounds
            if resultant_int > 1000 or resultant_int < 0:
                print("Chunk arguments should be in 0 < chunk_argument <= 1000")
                return None
        # Passed argument wasn't a string
        except ValueError:
            print(error_str)
            return None
    return resultant_int
