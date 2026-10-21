"""Translate known IBKR outcomes without hiding dependency/configuration failures."""

from src.utils.exception import ServiceError
import re
from src.utils.error_diagnostics import safe_diagnostic_text, safe_upstream_details


def _response_diagnostics(response, payload):
    error = payload.get('error')
    error = error if isinstance(error, dict) else {}
    message = next((value for value in (
        payload.get('detail'), payload.get('message'), payload.get('error_description'),
        payload.get('error'), error.get('message'), error.get('detail'), payload.get('title'),
    ) if isinstance(value, str) and value.strip()), '')
    if not message:
        message = ' | '.join(_validation_messages(payload))
    # Non-JSON gateway failures still need a useful, bounded explanation.
    if not message:
        raw = getattr(response, 'text', '')
        if isinstance(raw, str) and len(raw) <= 2048:
            message = re.sub(r'<[^>]*>', ' ', raw)
    headers = getattr(response, 'headers', {})
    request_id = payload.get('requestId') or payload.get('traceId')
    if not request_id and hasattr(headers, 'get'):
        request_id = next((headers.get(key) for key in ('X-Request-ID', 'X-Correlation-ID', 'request-id') if headers.get(key)), None)
    return safe_upstream_details({
        'ibkr_status': response.status_code,
        'ibkr_message': message or getattr(response, 'reason', ''),
        'ibkr_error_code': payload.get('code') or payload.get('errorCode') or error.get('code'),
        'ibkr_request_id': request_id,
    })


def _validation_messages(payload):
    messages = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == 'error' and isinstance(value, list):
                messages.extend(
                    item['value'][:500] for item in value
                    if isinstance(item, dict) and isinstance(item.get('value'), str)
                )
            elif isinstance(value, dict):
                messages.extend(_validation_messages(value))
    return messages[:10]


def raise_ibkr_response_error(response, *, trading_session=False, account_access=False, validation=True):
    """Called only after an endpoint has rejected the response status.

    A trading session is separate from AGM login and from IBKR OAuth credentials.
    Only known session/validation/account authorization outcomes become 4xx.
    Unknown upstream errors, OAuth errors, and reference-data access failures
    remain reportable 502s. Never return or log the raw upstream response here.
    """
    status = response.status_code
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    payload = payload if isinstance(payload, dict) else {}
    detail = next((payload[key] for key in ('detail', 'message', 'error')
                   if isinstance(payload.get(key), str) and payload[key].strip()), '')
    details = _response_diagnostics(response, payload)

    if trading_session and status == 401:
        raise ServiceError(
            'The IBKR trading session has expired. Reconnect to IBKR and try again.',
            status_code=409, code='ibkr_session_expired', details=details,
        )
    if trading_session and status == 500 and detail.strip().lower() == 'please query /accounts first':
        raise ServiceError(
            'The IBKR trading session is not ready. Reconnect to IBKR and try again.',
            status_code=409, code='ibkr_session_not_ready', details=details,
        )
    if status == 403 and account_access:
        raise ServiceError(
            'IBKR access is not enabled for this account. Check the selected master account and permissions.',
            status_code=403, code='ibkr_account_unauthorized', details=details,
        )
    if validation and status in (400, 422):
        messages = _validation_messages(payload)
        if messages:
            details['validation_errors'] = messages
        raise ServiceError(
            detail[:500] or 'IBKR rejected the request. Review the supplied information and try again.',
            status_code=status, code='ibkr_validation_error', details=details,
        )
    raise ServiceError(
        'The IBKR request could not be completed. Please try again later or contact support.',
        status_code=502, code='ibkr_upstream_error', details=details,
    )
