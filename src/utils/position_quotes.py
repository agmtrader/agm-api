"""Join reporting positions to uniquely identified stored market snapshots."""
from decimal import Decimal, InvalidOperation
import math
import re


def _text(value):
    return '' if value is None else str(value).strip()


def _conid(value):
    try:
        number = Decimal(_text(value))
        return str(int(number)) if number.is_finite() and number == int(number) and number > 0 else ''
    except (InvalidOperation, ValueError, OverflowError):
        return ''


def _price(value):
    try:
        number = float(str(value).replace(',', ''))
        return number if math.isfinite(number) and number > 0 else None
    except (ValueError, TypeError):
        return None


def enrich_position_quotes(positions, snapshots):
    """Do not fuzzy-match symbols, invent zero prices, or substitute last prices.

    snapshots contains records plus file name/modified time and asset class.
    Exact conid is preferred; symbol fallback requires asset class and currency.
    An ambiguous conid never falls back to a potentially different symbol.
    """
    by_conid, by_symbol = {}, {}
    for snapshot in snapshots:
        for row in snapshot['rows']:
            candidate = {
                'Bid': _price(row.get('Bid')), 'Ask': _price(row.get('Ask')),
                'QuoteCurrency': _text(row.get('Trading Currency')).upper() or None,
                'QuoteTimestamp': row.get('Timestamp') or row.get('_updated') or None,
                'QuoteSource': snapshot['name'],
                'QuoteSourceModifiedAt': snapshot.get('modified_time'),
            }
            conid = _conid(row.get('Conid'))
            if not conid and re.fullmatch(r'IBCID[0-9]+', _text(row.get('Symbol'))):
                conid = _conid(_text(row.get('Symbol'))[5:])
            if conid:
                by_conid.setdefault(conid, []).append(candidate)
            symbol = _text(row.get('Symbol')).upper()
            currency = candidate['QuoteCurrency']
            if symbol and currency:
                by_symbol.setdefault((snapshot['asset_class'], symbol, currency), []).append(candidate)
    result = []
    for position in positions:
        conid = _conid(position.get('Conid'))
        candidates = by_conid.get(conid, []) if conid else []
        currency = _text(position.get('CurrencyPrimary')).upper()
        if not candidates:
            key = (_text(position.get('AssetClass')).upper(), _text(position.get('Symbol')).upper(), currency)
            candidates = by_symbol.get(key, [])
        # Contracts carry quote currency; refuse conflicting/missing identity metadata.
        candidates = [c for c in candidates if not c['QuoteCurrency'] or c['QuoteCurrency'] == currency]
        empty = {'Bid': None, 'Ask': None, 'QuoteCurrency': None, 'QuoteTimestamp': None,
                 'QuoteSource': None, 'QuoteSourceModifiedAt': None}
        matched = len(candidates) == 1
        quote = candidates[0] if matched else empty
        status = 'matched' if matched and (quote['Bid'] is not None or quote['Ask'] is not None) else 'unavailable'
        if len(candidates) > 1:
            status = 'ambiguous'
        result.append({**position, **quote, 'QuoteStatus': status})
    return result
