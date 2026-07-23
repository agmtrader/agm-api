import functools
import traceback
import uuid

from flask import g, has_request_context, request

from .logger import logger


class ServiceError(Exception):
    """Standard application error that carries HTTP metadata."""

    def __init__(
        self,
        message: str = "Internal server error",
        status_code: int = 500,
        code: str | None = None,
        details: dict | None = None,
        error_id: str | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = details or {}
        self.error_id = error_id


def ensure_error_id(err: ServiceError) -> str:
    if err.error_id:
        return err.error_id

    request_id = getattr(g, 'request_id', None) if has_request_context() else None
    err.error_id = request_id or uuid.uuid4().hex[:12]
    return err.error_id


def get_request_context() -> dict:
    if not has_request_context():
        return {}

    return {
        'request_id': getattr(g, 'request_id', None),
        'method': request.method,
        'path': request.path,
        'endpoint': request.endpoint,
        'args': request.args.to_dict(flat=False),
        'remote_addr': request.headers.get('X-Forwarded-For', request.remote_addr),
    }


def build_error_payload(err: ServiceError) -> dict:
    payload = {
        'error': str(err),
        'error_id': ensure_error_id(err),
    }

    if err.code:
        payload['code'] = err.code

    if err.details:
        payload['details'] = err.details

    return payload


def log_service_error(err: ServiceError, source: str) -> None:
    error_id = ensure_error_id(err)
    context = get_request_context()
    logger.error(
        f"{source} failed [error_id={error_id}, status={err.status_code}, code={err.code or 'n/a'}]: {err}. "
        f"context={context} details={err.details}"
    )


def wrap_unhandled_exception(exc: Exception, source: str) -> ServiceError:
    error_id = uuid.uuid4().hex[:12]
    context = get_request_context()
    logger.exception(
        f"Unhandled error in {source} [error_id={error_id}]: {exc}. "
        f"context={context}\nTraceback:\n{traceback.format_exc()}"
    )
    return ServiceError(
        message="Internal server error",
        status_code=500,
        code="internal_error",
        error_id=error_id,
    )


def handle_exception(func):
    """Decorator for component/service layer functions."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ServiceError as err:
            if err.status_code >= 500:
                log_service_error(err, func.__name__)
            raise
        except Exception as exc:
            raise wrap_unhandled_exception(exc, func.__name__) from exc

    return wrapper
