from src.components.tools.email import Gmail
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import ast
import pandas as pd
from src.utils.exception import handle_exception

@handle_exception
def get_message_center_emails():

    # MESSAGE CENTER
    gmail = Gmail()
    emails = gmail.get_inbox_emails_from_sender('info@agmtechnology.com', include_body=True)
    emails_df = pd.DataFrame(emails)

    # Keep only emails from the last 30 days
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
    if 'date_ts' in emails_df.columns:
        emails_df = emails_df[emails_df['date_ts'] >= cutoff_ts]

    emails_df.to_csv('emails.csv', index=False)

    emails_dict = emails_df.to_dict(orient='records')

    pending_tasks = []

    for email in emails_dict:
        if email['subject'] == 'Message Summary':
            body_raw = email.get('body', '')
            body_dict = {}
            if isinstance(body_raw, dict):
                body_dict = body_raw
            elif isinstance(body_raw, str):
                try:
                    body_dict = ast.literal_eval(body_raw)
                except Exception:
                    body_dict = {}

            html_body = body_dict.get('html') or body_dict.get('text', '')
            rows = _parse_message_summary_table(html_body)
            for row in rows:
                pending_tasks.append(row)

    return pending_tasks


def _parse_message_summary_table(html: str):
    """Return list of dicts extracted from the message-summary <table border="1">"""
    rows = []
    if not html or not isinstance(html, str):
        return rows
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', attrs={'border': '1'})
    if not table:
        return rows

    # Determine headers by reading first row
    header_cells = table.find('tr').find_all(['th', 'td']) if table.find('tr') else []
    headers = [c.get_text(strip=True).lower() for c in header_cells]

    def col(name):
        lname = name.lower()
        for idx, h in enumerate(headers):
            if lname in h:
                return idx
        return None

    for tr in table.find_all('tr')[1:]:  # skip header
        tds = tr.find_all('td')
        if not tds:
            continue
        def safe(idx):
            return tds[idx].get_text(strip=True) if idx is not None and idx < len(tds) else ''
        rows.append({
            'subject': safe(col('subject')),
            'category': safe(col('category')),
            'unread_count': safe(col('unread')),
            'newest_date': safe(col('newest')),
        })
    return rows
