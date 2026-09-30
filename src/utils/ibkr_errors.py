"""Translate known IBKR outcomes without hiding dependency/configuration failures."""

from src.utils.exception import ServiceError


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
    details = {'ibkr_status': status}

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
