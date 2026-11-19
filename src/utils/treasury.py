"""Identify Treasury securities from server-read holding metadata."""
import re


_TREASURY_IDENTIFIER = re.compile(
    r'\b(?:US)?(?:912795|912796|912828|91282C|912810|912820|912821|912803|912800|912833|912834|912815)[A-Z0-9]{3}\d?\b',
    re.IGNORECASE,
)
_TREASURY_NAME = re.compile(r'\b(?:U\.?S\.?[\s-]+TREASURY|UNITED STATES TREASURY)\b|^UST\s+\d', re.IGNORECASE)
_TREASURY_SYMBOL = re.compile(r'T\s+\d+(?:\.\d+|\s+\d+/\d+)?\s+\d{1,2}/\d{1,2}/\d{2,4}', re.IGNORECASE)


def is_treasury_position(position):
    security_type = str(position.get('secType') or position.get('assetClass') or '').upper()
    if security_type == 'BILL':
        return True
    if security_type != 'BOND':
        return False
    description = str(position.get('description') or position.get('contractDesc') or '').strip()
    identifiers = [position.get(key) for key in ('isin', 'ISIN', 'cusip', 'CUSIP', 'securityId', 'securityID')]
    identity = ' '.join([description, *(value for value in identifiers if isinstance(value, str))])
    return bool(_TREASURY_IDENTIFIER.search(identity) or _TREASURY_NAME.search(description)
                or _TREASURY_SYMBOL.fullmatch(description))
