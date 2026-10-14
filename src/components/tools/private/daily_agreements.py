"""Preparation and submission for the daily agreement preload workflow."""
from datetime import datetime, timezone
from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI, DAILY_AGREEMENT_FORM_NUMBERS


def _run_daily_agreements(payload, *, dry_run):
    if not isinstance(payload, dict) or set(payload) != {'master_account'}:
        raise ServiceError('Provide only master_account', 422)
    master = payload.get('master_account')
    if not isinstance(master, str) or master.strip().upper() not in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
        raise ServiceError('A supported master_account is required', 422)
    started = datetime.now(timezone.utc).isoformat()
    result = IBKRWebAPI().submit_all_agreements(master_account=master.strip().upper(), dry_run=dry_run)
    keys = ('ok','ready','submitted','masterAccount','formsRequested','documentsPrepared',
            'accepted','rejected','skipped','requestBytes','results')
    return {**{key: result[key] for key in keys if key in result},
            'mode': 'preparation' if dry_run else 'submission', 'expectedForms': list(DAILY_AGREEMENT_FORM_NUMBERS),
            'startedAt': started, 'completedAt': datetime.now(timezone.utc).isoformat()}


@handle_exception
def prepare_daily_agreements(payload):
    return _run_daily_agreements(payload, dry_run=True)


@handle_exception
def submit_daily_agreements(payload):
    return _run_daily_agreements(payload, dry_run=False)
