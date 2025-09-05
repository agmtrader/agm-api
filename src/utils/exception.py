from .logger import logger
import functools


class ServiceError(Exception):
    """Standard application error that carries an HTTP status code."""

    def __init__(self, message: str = "Internal server error", status_code: int = 500):
        super().__init__(message)
        self.status_code = status_code


def handle_exception(func):
    """Decorator for component/service layer functions.

    * On success: returns the function's return value unchanged.
    * On known failure: propagate ServiceError unchanged.
    * On unexpected failure: log it and convert to ServiceError(500).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ServiceError:
            # Allow explicit ServiceError to bubble up unchanged.
            raise
        except Exception as exc:
            logger.error(f"Unhandled error in {func.__name__}: {exc}")
            # Wrap unexpected exceptions so upper layers can format consistently.
            raise ServiceError() from exc

    return wrapper