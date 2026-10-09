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
        self._logged = False


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
        'path': request.url_rule.rule if request.url_rule else '<unmatched>',
        'endpoint': request.endpoint,
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


def _safe_traceback(exc: Exception, seen: set[int] | None = None) -> str:
    """Preserve reportable Python stack locations without messages or locals.

    Upstream exception strings can contain tokens, URLs, documents or client
    payloads. Source lines can also contain literal secrets, so retain only
    the filename, line number and function name for each frame.
    """
    seen = set() if seen is None else seen
    if id(exc) in seen:
        return ""
    seen.add(id(exc))
    previous = exc.__cause__
    separator = "The above exception was the direct cause of the following exception:"
    if previous is None and not exc.__suppress_context__:
        previous = exc.__context__
        separator = "During handling of the above exception, another exception occurred:"
    output = ""
    if previous is not None and id(previous) not in seen:
        output = _safe_traceback(previous, seen) + "\n" + separator + "\n\n"
    output += "Traceback (most recent call last):\n"
    for frame in traceback.extract_tb(exc.__traceback__):
        output += f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}\n'
    exception_type = type(exc)
    output += f"{exception_type.__module__}.{exception_type.__qualname__}: message omitted\n"
    return output


def log_service_error(err: ServiceError, source: str) -> None:
    # A single failure can cross several decorated components and a route.
    # Expected request failures never need an exception report.
    if err.status_code < 500 or err._logged:
        return
    error_id = ensure_error_id(err)
    context = get_request_context()
    logger.error(
        f"{source} failed [error_id={error_id}, status={err.status_code}, code={err.code or 'n/a'}]. "
        f"context={context}\n{_safe_traceback(err)}"
    )
    err._logged = True


def wrap_unhandled_exception(exc: Exception, source: str) -> ServiceError:
    error_id = uuid.uuid4().hex[:12]
    context = get_request_context()
    logger.error(
        f"Unhandled error in {source} [error_id={error_id}]. "
        f"context={context}\n{_safe_traceback(exc)}"
    )
    err = ServiceError(
        message="Internal server error",
        status_code=500,
        code="internal_error",
        error_id=error_id,
    )
    err._logged = True
    return err


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
