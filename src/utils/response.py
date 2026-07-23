from functools import wraps

from flask import Response, jsonify

from src.utils.exception import (
    ServiceError,
    build_error_payload,
    log_service_error,
    wrap_unhandled_exception,
)


def format_response(func):
    """Decorator for Flask route functions."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            payload = func(*args, **kwargs)

            if isinstance(payload, Response):
                return payload
            if isinstance(payload, tuple) and len(payload) == 2 and isinstance(payload[0], Response):
                return payload

            return jsonify(payload), 200
        except ServiceError as err:
            if err.status_code >= 500:
                log_service_error(err, func.__name__)
            return jsonify(build_error_payload(err)), err.status_code
        except Exception as exc:
            err = wrap_unhandled_exception(exc, func.__name__)
            return jsonify(build_error_payload(err)), err.status_code

    return wrapper
