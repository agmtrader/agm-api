from flask import Response
import json
import functools

def format_response(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        data = func(*args, **kwargs)
        # If the data is already a Response object (e.g., an error response from handle_exception), return it directly
        if isinstance(data, Response):
            return data
        return Response(json.dumps(data), status=200, mimetype='application/json')
    return wrapper 