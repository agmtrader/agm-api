from functools import wraps

from flask import jsonify, Response

from src.utils.exception import ServiceError


def format_response(func):
    """Decorator for Flask route functions.

    Converts return value to `jsonify` Response and handles `ServiceError`.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            payload = func(*args, **kwargs)

            # If route already produced a Response or (Response, status) pass through.
            if isinstance(payload, Response):
                return payload
            if isinstance(payload, tuple) and len(payload) == 2 and isinstance(payload[0], Response):
                return payload

            # Otherwise convert dictionary/list/etc to JSON response with 200.
            return jsonify(payload), 200
        except ServiceError as err:
            return jsonify({"error": str(err)}), err.status_code

    return wrapper 