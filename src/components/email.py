from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from jinja2 import Environment, FileSystemLoader
from premailer import transform

from src.utils.logger import logger
from src.utils.exception import handle_exception
from src.utils.secret_manager import get_secret

import os
import base64

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
  def create_html_email(self, plain_text, subject, client_email):

    logger.info(f'Creating HTML email with subject: {subject}')

    # Get the template html file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(os.path.join(current_dir, '../lib/email_templates')))
    template = env.get_template('trade_ticket.html')

    # Render the template with the plain text content
    html_content = template.render(content=plain_text, subject=subject)

    # Inline the CSS
    html_content_inlined = transform(html_content)

    # Create a multipart message
    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = "info@agmtechnology.com"

    # Attach plain text and HTML versions
    text_part = MIMEText(plain_text, 'plain')
    html_part = MIMEText(html_content_inlined, 'html')
    
    message.attach(text_part)
    message.attach(html_part)

    # Create the final multipart message
    final_message = MIMEMultipart('related')
    final_message['Subject'] = subject
    final_message['To'] = client_email
    final_message['From'] = "info@agmtechnology.com"
    final_message['Bcc'] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com"

    final_message.attach(message)

    # Attach the logo image
    logo_path = 'public/assets/brand/agm-logo.png'
    with open(logo_path, 'rb') as logo_file:
        logo_mime = MIMEImage(logo_file.read())
        logo_mime.add_header('Content-ID', '<logo>')
        final_message.attach(logo_mime)

    logger.success(f'Successfully created HTML email with subject: {subject}')
    raw_message = base64.urlsafe_b64encode(final_message.as_bytes()).decode()
    return {'raw': raw_message}

  @handle_exception
  def send_client_email(self, plain_text, client_email, subject):

    logger.info(f'Sending client email to: {client_email}')
    raw_message = self.create_html_email(plain_text, subject, client_email)

    send_message = (
        self.service.users()
        .messages()
        .send(userId="me", body=raw_message)
        .execute()
    )
    logger.success(f'Successfully sent client email to: {client_email}')
    return {'emailId': send_message["id"]}