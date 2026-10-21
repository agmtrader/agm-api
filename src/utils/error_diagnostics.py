"""Bounded, sanitized provider diagnostics shared with dependency-free Actions scripts."""
import re


def safe_diagnostic_text(value, limit=500):
    """Bound provider error fields; never retain credentials, PDFs or envelopes."""
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        return ''
    text = str(value)[:8192]
    text = re.sub(r'-----BEGIN[^-]*-----.*?(?:-----END[^-]*-----|$)', '[redacted key]', text, flags=re.S)
    text = re.sub(r'(?i)\b(?:bearer|basic)\s+[^\s,;<>]+', '[redacted authorization]', text)
    text = re.sub(r'''(?ix)(["']?(?:access_token|refresh_token|token|password|authorization|client_secret|api_key|data)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}<>]+)''', r'\1[redacted]', text)
    text = re.sub(r'[A-Za-z0-9_+/=-]{80,}', '[redacted encoded data]', text)
    text = re.sub(r'\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b', '[redacted email]', text)
    text = re.sub(r'\b[UIF]\d{6,}\b', '[redacted account]', text)
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', text)
    return ' '.join(text.split())[:limit]


def safe_upstream_details(details):
    """Shared allowlist for API logs and the standalone Actions receipt."""
    if not isinstance(details, dict):
        return {}
    allowed = {}
    status = details.get('ibkr_status')
    if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599:
        allowed['ibkr_status'] = status
    for key in ('ibkr_message', 'ibkr_error_code', 'ibkr_request_id'):
        value = safe_diagnostic_text(details.get(key), 500 if key == 'ibkr_message' else 120)
        if value:
            allowed[key] = value
    for key in ('agreement_batch', 'agreement_batches', 'agreement_signed_bytes'):
        value = details.get(key)
        if type(value) is int and 0 <= value <= 20_000_000:
            allowed[key] = value
    for key in ('agreement_accepted_forms', 'agreement_failed_batch_forms', 'agreement_unsubmitted_forms'):
        values = details.get(key)
        if isinstance(values, list):
            allowed[key] = [str(value) for value in values[:100] if re.fullmatch(r'\d{1,8}', str(value))]
    if isinstance(details.get('agreement_completed_batches'), list):
        allowed['agreement_completed_batches'] = safe_agreement_batches(details['agreement_completed_batches'])
    return allowed


def safe_agreement_batches(batches):
    if not isinstance(batches, list):
        return []
    result = []
    for row in batches[:100]:
        if not isinstance(row, dict):
            continue
        record = {}
        for key in ('batch','requestBytes','signedBytes','signedBytesBound'):
            if type(row.get(key)) is int:
                record[key] = row[key]
        for key in ('requestId','dateSubmitted'):
            if key in row:
                record[key] = safe_diagnostic_text(row[key], 120)
        if isinstance(row.get('formNumbers'), list):
            record['formNumbers'] = [str(value) for value in row['formNumbers'][:100] if re.fullmatch(r'\d{1,8}', str(value))]
        result.append(record)
    return result
