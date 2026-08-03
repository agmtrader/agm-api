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

    def _format_payload(payload):
        """Convert a view return value to a Flask response.

        Flask view functions may return a response object or a tuple of
        ``(body, status)``, ``(body, headers)``, or
        ``(body, status, headers)``.  The previous implementation only
        recognized tuples whose body was already a ``Response``.  As a
        result, routes returning ``({'error': '...'}, 400)`` were serialized
        as a JSON array and sent with HTTP 200.
        """
        if isinstance(payload, Response):
            return payload

        if isinstance(payload, tuple):
            if len(payload) not in (2, 3):
                raise TypeError('View tuple responses must contain 2 or 3 items')

            body = payload[0]
            if isinstance(body, Response):
                return payload

            response = jsonify(body)
            if len(payload) == 2:
                return response, payload[1]
            return response, payload[1], payload[2]

        return jsonify(payload), 200

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            payload = func(*args, **kwargs)
            return _format_payload(payload)
        except ServiceError as err:
            if err.status_code >= 500:
                log_service_error(err, func.__name__)
            return jsonify(build_error_payload(err)), err.status_code
        except Exception as exc:
            err = wrap_unhandled_exception(exc, func.__name__)
            return jsonify(build_error_payload(err)), err.status_code

    return wrapper
