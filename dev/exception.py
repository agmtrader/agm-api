from .logger import logger
from flask import Response
import json

def handle_exception(func):
    def wrapper(*args, **kwargs):
        try:
            data = func(*args, **kwargs)
            return Response(json.dumps(data), status=200)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            json_data = json.dumps({'error': str(e)})
            return Response(json_data, status=500)
    return wrapper