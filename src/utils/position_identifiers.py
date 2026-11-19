"""Attach reference identifiers without replacing live position data."""
from decimal import Decimal, InvalidOperation


def _contract_id(value):
    try:
        number = Decimal(str(value).strip())
        return str(int(number)) if number.is_finite() and number > 0 and number == int(number) else ''
    except (InvalidOperation, ValueError, OverflowError):
        return ''


def enrich_position_identifiers(positions, reference_rows, account_id):
    by_contract = {}
    for row in reference_rows:
        if str(row.get('ClientAccountID') or '').strip() != account_id:
            continue
        conid = _contract_id(row.get('Conid'))
        if not conid:
            continue
        fields = by_contract.setdefault(conid, {'isin': set(), 'cusip': set(), 'securityId': set()})
        for target, source in (('isin', 'ISIN'), ('cusip', 'CUSIP'), ('securityId', 'SecurityID')):
            value = row.get(source)
            if isinstance(value, str) and value.strip() and value.strip().lower() not in ('nan', 'none'):
                fields[target].add(value.strip().upper())

    result = []
    for position in positions:
        enriched = dict(position)
        fields = by_contract.get(_contract_id(position.get('conid')), {})
        ambiguous = any(len(values) > 1 for values in fields.values())
        for key, values in fields.items():
            aliases = (key, key.upper(), 'securityID') if key == 'securityId' else (key, key.upper())
            existing = {str(position[alias]).strip().upper() for alias in aliases if position.get(alias)}
            if existing and values and existing != values:
                ambiguous = True
        if not ambiguous:
            for key, values in fields.items():
                aliases = (key, key.upper(), 'securityID') if key == 'securityId' else (key, key.upper())
                if values and not any(position.get(alias) for alias in aliases):
                    enriched[key] = next(iter(values))
        enriched['identifierStatus'] = 'ambiguous' if ambiguous else ('matched' if any(fields.values()) else 'unavailable')
        result.append(enriched)
    return result
