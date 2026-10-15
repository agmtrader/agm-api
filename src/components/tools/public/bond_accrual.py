"""Verified coupon schedules used by ACOBO trade confirmations.

Keep this registry security-specific: currency and maturity alone do not establish
a bond's coupon frequency or day-count convention.
"""
from datetime import date

from src.utils.exception import ServiceError


VERIFIED_ANNUAL_ACTUAL_BONDS = {
    'XS1046593908': {
        'coupon_month': 3,
        'coupon_day': 19,
        'issue_date': date(2014, 3, 19),
        'maturity': date(2114, 3, 19),
        'source': 'https://www.sec.gov/Archives/edgar/data/101368/000119312514095596/d692806dfwp.htm',
    },
}


def _isin(row):
    isin = str(row.get('ISIN') or '').strip().upper()
    security_isin = ''
    if str(row.get('SecurityIDType') or '').strip().upper() == 'ISIN':
        security_isin = str(row.get('SecurityID') or '').strip().upper()
    return isin, security_isin


def verified_accrued_days(rows):
    """Return actual days for a verified bond, or None for the legacy path.

    Actual/Actual counts calendar days from the previous scheduled coupon to
    settlement. No inversion of rounded broker interest or fixed year is needed.
    Unknown securities retain the existing inferred-360 behavior at the caller.
    """
    identities = [_isin(row) for row in rows]
    known = {value for pair in identities for value in pair
             if value in VERIFIED_ANNUAL_ACTUAL_BONDS}
    if not known:
        return None

    def reject(message):
        raise ServiceError(message, status_code=400, code='invalid_bond_accrual')

    if len(known) != 1:
        reject('Select executions with the same verified bond coupon schedule.')
    isin = next(iter(known))
    if any(not any(pair) or any(value and value != isin for value in pair)
           for pair in identities):
        reject('Select executions with the same verified bond ISIN.')
    try:
        settlements = {date.fromisoformat(str(row.get('SettleDate') or '')) for row in rows}
    except ValueError:
        reject('A valid settlement date is required to calculate accrued days.')
    if len(settlements) != 1:
        reject('Select executions with the same settlement date to consolidate accrued days.')

    settlement = next(iter(settlements))
    terms = VERIFIED_ANNUAL_ACTUAL_BONDS[isin]
    if not terms['issue_date'] <= settlement < terms['maturity']:
        reject('Settlement must fall within the verified bond accrual schedule.')
    previous_coupon = date(settlement.year, terms['coupon_month'], terms['coupon_day'])
    if settlement < previous_coupon:
        previous_coupon = previous_coupon.replace(year=settlement.year - 1)
    return (settlement - previous_coupon).days
