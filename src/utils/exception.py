from src.utils.response import Response
from src.utils.logger import logger

def handle_exception(func):
    def wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            return response
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            return e
    return wrapper