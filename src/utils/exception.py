from .logger import logger
from flask import Response
import json
import functools

def handle_exception(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            data = func(*args, **kwargs)
            return data
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            json_data = json.dumps({'error': str(e)})
            return Response(json_data, status=500, mimetype='application/json')
    return wrapper