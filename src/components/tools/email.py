from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.utils import formataddr, parsedate_to_datetime
from jinja2 import Environment, FileSystemLoader
from premailer import transform
from bs4 import BeautifulSoup

from src.utils.logger import logger
from src.utils.exception import handle_exception
from src.utils.managers.secret_manager import get_secret

import os
import base64
import re
from datetime import datetime, timezone

class Gmail:

  def __init__(self):
    logger.announcement('Initializing Gmail connection.', type='info')
    SCOPES = ["https://mail.google.com/"]
    creds = get_secret('OAUTH_PYTHON_CREDENTIALS_INFO')
    try:
      creds = Credentials(
        token=creds['token'],
        refresh_token=creds['refresh_token'],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        scopes=SCOPES
      )
      self.service = build("gmail", "v1", credentials=creds)
      logger.announcement('Initialized Gmail connection.', type='success')
    except Exception as e:
      logger.error(f"Error initializing Gmail: {str(e)}")
      raise Exception(f"Error initializing Gmail: {str(e)}")
    
  @handle_exception
  def send_email(self, content, client_email, subject, email_template, bcc="aa@agmtechnology.com,cr@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com", cc=""):
    """
    Send an email using either plain text or dictionary content.
    
    Args:
        content: Can be either a string (plain text) or a dictionary (structured data)
        client_email: Recipient email address
        subject: Email subject
        email_template: Name of the template file to use
        bcc: Bcc email address
        cc: Cc email address
    """
    logger.announcement(f'Sending {email_template} email to: {client_email}', type='info')
    raw_message = self.create_html_email(content, subject, client_email, email_template, bcc, cc)

    sent_message = (
        self.service.users()
        .messages()
        .send(userId="me", body=raw_message)
        .execute()
    )
    logger.announcement(f'Successfully sent {email_template} email to: {client_email}', type='success')
    return sent_message['id']
  
  def create_html_email(self, content, subject, client_email, email_template, bcc, cc):
    """
    Create an HTML email that can handle both plain text and dictionary content.
    
    Args:
        content: Can be either a string (plain text) or a dictionary (structured data)
        subject: Email subject
        client_email: Recipient email address
        email_template: Name of the template file to use
    """
    logger.info(f'Creating {email_template} email with subject: {subject}')

    # Get the template html file
    try:
      current_dir = os.path.dirname(os.path.abspath(__file__))
      env = Environment(loader=FileSystemLoader(os.path.join(current_dir, '../../lib/email_templates')))
      template = env.get_template(f'{email_template}.html')
    except:
      logger.error(f'Template {email_template}.html not found')
      raise Exception(f'Template {email_template}.html not found')

    # Prepare the content based on its type
    template_data = {
        'subject': subject,
        'content': content
    }

    # Render the template with the content
    html_content = template.render(**template_data)

    # Inline the CSS
    html_content_inlined = transform(html_content)

    # Create a multipart message
    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    display_name = "AGM Technology"
    email_address = "info@agmtechnology.com"
    formatted_from = formataddr((display_name, email_address))
    message['From'] = formatted_from

    # Convert dictionary to readable plain text format
    if isinstance(content, dict):
        text_content = "\n".join(f"{key}: {value}" for key, value in content.items())
        text_part = MIMEText(text_content.encode('utf-8'), 'plain', 'utf-8')
        message.attach(text_part)

    html_part = MIMEText(html_content_inlined.encode('utf-8'), 'html', 'utf-8')
    message.attach(html_part)

    # Create the final multipart message
    final_message = MIMEMultipart('related')
    final_message['Subject'] = subject
    final_message['To'] = client_email
    final_message['From'] = formatted_from
    final_message['Bcc'] = bcc
    final_message['Cc'] = cc

    final_message.attach(message)

    # Attach the logo image
    logo_path = 'public/assets/brand/agm-logo.png'
    with open(logo_path, 'rb') as logo_file:
        logo_mime = MIMEImage(logo_file.read())
        logo_mime.add_header('Content-ID', '<logo>')
        final_message.attach(logo_mime)

    logger.success(f'Successfully created {email_template} email with subject: {subject}')
    raw_message = base64.urlsafe_b64encode(final_message.as_bytes()).decode()
    return {'raw': raw_message}

  @handle_exception
  def send_trade_ticket_email(self, content, client_email):
    subject = 'Confirmación de Transacción'
    email_template = 'trade_ticket'
    return self.send_email(content, client_email, subject, email_template)

  @handle_exception
  def send_email_change_email(self, client_email: str, advisor_email: str = None):
    subject = 'Urgente: Actualización de Correo Electrónico'
    email_template = 'email_change'
    bcc = ""
    cc = f"jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com"
    if advisor_email:
      cc += f",{advisor_email}"
    return self.send_email("", client_email, subject, email_template, bcc=bcc, cc=cc)

  @handle_exception
  def send_email_confirmation(self, content, client_email, lang='es'):
    subject = 'Confirmación de Correo Electrónico' if lang == 'es' else 'Email Confirmation'
    email_template = f'application_email_confirmation_{lang}'
    bcc = ""
    cc = ""
    return self.send_email(content, client_email, subject, email_template, bcc=bcc, cc=cc)
  
  @handle_exception
  def send_application_link_email(self, content, client_email, lang='es'):
    subject = 'Link de formulario para apertura de cuenta' if lang == 'es' else 'Application Link'
    email_template = f'application_link_{lang}'
    bcc = ""
    cc = "jc@agmtechnology.com,hc@agmtechnology.com"
    return self.send_email(content, client_email, subject, email_template, bcc=bcc, cc=cc)

  @handle_exception
  def send_task_reminder_email(self, content, agm_user_email):
    subject = 'Task Reminder'
    email_template = 'task_reminder'
    bcc = ""
    cc = ""
    return self.send_email(content, agm_user_email, subject, email_template, bcc=bcc, cc=cc)

  @handle_exception
  def get_inbox_emails_from_sender(self, sender: str, include_body: bool = False):
    """
    Read all emails in the inbox that match the provided sender.

    Args:
      sender: Email address (or partial) of the sender to search for, e.g. "noreply@example.com".
      include_body: If True, returns decoded text/html bodies when available.

    Returns:
      List of dictionaries with message metadata (and body if requested).
    """
    logger.announcement(f'Fetching inbox emails from sender: {sender}', type='info')

    query = f'from:{sender} in:inbox'
    user_id = 'me'
    messages = []

    page_token = None
    while True:
      request = self.service.users().messages().list(userId=user_id, q=query, pageToken=page_token)
      response = request.execute()

      for item in response.get('messages', []):
        msg = self.service.users().messages().get(
          userId=user_id,
          id=item['id'],
          format='full' if include_body else 'metadata',
          metadataHeaders=['Subject', 'From', 'To', 'Date']
        ).execute()

        headers = msg.get('payload', {}).get('headers', []) if msg.get('payload') else []
        subject = self._get_header(headers, 'Subject')
        from_header = self._get_header(headers, 'From')
        to_header = self._get_header(headers, 'To')
        date_header = self._get_header(headers, 'Date')

        # Base message fields
        message_entry = {
          'id': msg.get('id'),
          'threadId': msg.get('threadId'),
          'snippet': msg.get('snippet'),
          'subject': subject,
          'from': from_header,
          'to': to_header,
          'date': date_header,
        }

        if include_body:
          body = self._extract_message_body(msg.get('payload', {}))
          message_entry['body'] = body

        # Enrichment: parsed date (tz-aware), epoch seconds and ISO
        try:
          parsed_dt = parsedate_to_datetime(date_header) if date_header else None
          if parsed_dt and parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
          if parsed_dt:
            message_entry['date_iso'] = parsed_dt.astimezone(timezone.utc).isoformat()
            message_entry['date_ts'] = int(parsed_dt.timestamp())
        except Exception:
          pass

        # Enrichment: category and details
        category = self._categorize_message(subject or '', from_header or '')
        message_entry['category'] = category
        try:
          details = self._extract_details(category, subject or '', message_entry.get('snippet') or '', (message_entry.get('body') or {}).get('text', ''), (message_entry.get('body') or {}).get('html', ''))
          if details:
            message_entry['details'] = details
        except Exception:
          # Non-fatal if extraction fails
          pass

        messages.append(message_entry)

      page_token = response.get('nextPageToken')
      if not page_token:
        break

    logger.success(f'Fetched {len(messages)} emails from sender: {sender}')
    return messages

  def _extract_message_body(self, payload):
    """
    Extract text and html bodies from a Gmail message payload.

    Returns a dict: { 'text': str, 'html': str }
    """
    result = {'text': '', 'html': ''}

    if not payload:
      return result

    # Handle single-part messages
    body = payload.get('body', {})
    data = body.get('data')
    mime_type = payload.get('mimeType', '')
    if data:
      try:
        decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
      except Exception:
        decoded = ''
      if mime_type == 'text/plain':
        result['text'] += decoded
      elif mime_type == 'text/html':
        result['html'] += decoded

    # Handle multi-part messages recursively
    def walk(parts):
      for part in parts or []:
        part_mime = part.get('mimeType', '')
        part_body = part.get('body', {})
        part_data = part_body.get('data')
        if part_data:
          try:
            part_decoded = base64.urlsafe_b64decode(part_data).decode('utf-8', errors='ignore')
          except Exception:
            part_decoded = ''
          if part_mime == 'text/plain':
            result['text'] += part_decoded
          elif part_mime == 'text/html':
            result['html'] += part_decoded
        # Nested parts
        if part.get('parts'):
          walk(part.get('parts'))

    if payload.get('parts'):
      walk(payload.get('parts'))

    return result

  def _get_header(self, headers, name):
    for header in headers or []:
      if header.get('name', '').lower() == name.lower():
        return header.get('value')
    return None

  def _categorize_message(self, subject: str, from_header: str) -> str:
    subj = subject.lower()
    if 'message center notification' in subj:
      return 'message_center_notification'
    if 'message summary' in subj:
      return 'message_summary'
    if 'mutual fund/etf advisory' in subj:
      return 'fund_etf_advisory'
    if 'earnings notification' in subj:
      return 'earnings_notification'
    if 'option expiration notification' in subj:
      return 'options_expiration'
    if 'dividend-triggered option exercise advisory' in subj:
      return 'dividend_option_exercise'
    if 'monthly activity statement' in subj:
      return 'monthly_statement'
    if 'reminder to think before you click' in subj:
      return 'security_reminder'
    if 'important message' in subj:
      return 'important_account_alert'
    if subj.startswith('fyi: changes in analyst ratings') or 'analyst ratings' in subj:
      return 'analyst_ratings'
    if 'confirmation of request' in subj:
      return 'request_confirmation'
    return 'other'

  def _extract_details(self, category: str, subject: str, snippet: str, body_text: str, body_html: str = ''):
    text = ' '.join([subject or '', snippet or '', body_text or ''])
    text = re.sub(r'\s+', ' ', text)
    if category == 'earnings_notification':
      # Example: "- NVDA declaring Q2 '26 earning on 27-AUG-2025 AfterClose. ... Implied Move: +/-6.8%."
      m = re.search(r'-\s*([A-Z]{1,6})\s+declaring\s+(Q\d+\s*[\'\u2019]?\d{2})\s+earning\s+on\s+([0-9]{1,2}-[A-Z]{3}-[0-9]{4})\s+(BeforeOpen|AfterClose)', text, re.IGNORECASE)
      implied = re.search(r'Implied Move:\s*([+\-]?[0-9]+(?:\.[0-9]+)?)%', text)
      details = {}
      if m:
        details['ticker'] = m.group(1).upper()
        details['quarter'] = m.group(2).upper()
        details['date'] = m.group(3)
        details['session'] = m.group(4)
      if implied:
        details['implied_move_pct'] = float(implied.group(1))
      return details or None
    if category == 'options_expiration':
      # Capture a few option lines: "- TSLA 15AUG2025 200 P in Account(Qty): U****467(10)"
      option_items = re.findall(r'-\s*([A-Z]{1,6})\s+(\d{2}[A-Z]{3}\d{4})\s+([0-9]+)\s+([CP])\b', text)
      accounts = re.findall(r'U\*+\d+', text)
      if option_items or accounts:
        return {
          'options': [
            { 'ticker': t.upper(), 'expiry': exp, 'strike': int(strike), 'type': opt }
            for (t, exp, strike, opt) in option_items
          ],
          'accounts': list(dict.fromkeys(accounts))
        }
      return None
    if category == 'fund_etf_advisory':
      # "- IBIT in Account(Qty): U****416(70)"
      tickers = re.findall(r'-\s*([A-Z]{2,10})\b', text)
      accounts = re.findall(r'U\*+\d+', text)
      if tickers or accounts:
        return { 'tickers': list(dict.fromkeys([t.upper() for t in tickers])), 'accounts': list(dict.fromkeys(accounts)) }
      return None
    if category == 'message_summary':
      master = re.search(r'master account\s+([A-Z]\*+\d+)', text, re.IGNORECASE)
      # Parse html table for row data
      rows_data = []
      if body_html:
        try:
          soup = BeautifulSoup(body_html, 'html.parser')
          # find first table that has a header cell containing "Subject"
          table = None
          for tbl in soup.find_all('table'):
            th_texts = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
            if any('subject' in t for t in th_texts):
              table = tbl
              break
          if table:
            # identify column indices based on header labels
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            def col(name):
              for idx, h in enumerate(headers):
                if name in h:
                  return idx
              return None
            for tr in table.find_all('tr')[1:]:
              tds = tr.find_all(['td', 'th'])
              if not tds:
                continue
              def safe(idx):
                return tds[idx].get_text(strip=True) if idx is not None and idx < len(tds) else ''
              rows_data.append({
                'subject': safe(col('subject')),
                'category': safe(col('category')),
                'message_type': safe(col('message type')),
                'unread_count': safe(col('unread')),
                'newest_date': safe(col('newest')),
              })
        except Exception:
          pass
      unread = bool(rows_data)  # if rows exist, they are unread summary rows
      return { 'master_account': master.group(1) if master else None, 'rows': rows_data, 'has_unread': unread }
    if category == 'message_center_notification':
      master = re.search(r'Account\s+([A-Z]\*+\d+)', text, re.IGNORECASE)
      # Extract ticket numbers and descriptions from html body
      tickets = []
      if body_html:
        try:
          soup = BeautifulSoup(body_html, 'html.parser')
          # This assumes ticket numbers are inside elements containing "Ticket Number:" text
          for elem in soup.find_all(text=re.compile(r'Ticket Number', re.IGNORECASE)):
            ticket_text = elem.parent.get_text(" ", strip=True)
            m = re.search(r'Ticket Number[:\s]*([A-Z0-9]+)', ticket_text)
            if m:
              tickets.append({ 'ticket': m.group(1), 'description': ticket_text })
        except Exception:
          pass
      return { 'master_account': master.group(1) if master else None, 'tickets': tickets }
    if category == 'monthly_statement':
      month_year = re.search(r'Monthly Activity Statement for\s+([A-Za-z]+\s+\d{4})', text)
      account = re.search(r'account\s+([FU]\*+\d+)', text)
      return { 'period': month_year.group(1) if month_year else None, 'account': account.group(1) if account else None }
    if category == 'analyst_ratings':
      # No structured fields reliably present; return a simple marker
      return { 'topic': 'analyst_ratings_update' }
    if category == 'security_reminder':
      return { 'topic': 'security_phishing_reminder' }
    if category == 'important_account_alert':
      return { 'topic': 'important_action_required' }
    if category == 'request_confirmation':
      return { 'topic': 'request_confirmation' }
    return None