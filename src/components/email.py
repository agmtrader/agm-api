
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from jinja2 import Environment, FileSystemLoader
from premailer import transform

from src.utils.logger import logger
from src.utils.response import Response
from src.utils.exception import handle_exception
from flask import jsonify

import os
import base64

class Gmail:

  def __init__(self):
    logger.announcement('Initializing Gmail connection.', type='info')
    SCOPES = ["https://mail.google.com/"]
    try:
      creds = Credentials(
        token=os.getenv('INFO_TOKEN'),
        refresh_token=os.getenv('INFO_REFRESH_TOKEN'),
        token_uri=os.getenv('INFO_TOKEN_URI'),
        client_id=os.getenv('INFO_CLIENT_ID'),
        client_secret=os.getenv('INFO_CLIENT_SECRET'),
        scopes=SCOPES
      )
      self.service = build("gmail", "v1", credentials=creds)
      logger.announcement('Initialized Gmail connection.', type='success')
    except Exception as e:
      logger.error(f"Error initializing Gmail: {str(e)}")
      raise Exception(f"Error initializing Gmail: {str(e)}")

  @handle_exception
  def create_html_email(self, plain_text, subject):
    logger.info(f'Creating HTML email with subject: {subject}')

    # Load the HTML template
    # Get current directory
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

    # Create the final multipart/related message
    final_message = MIMEMultipart('related')
    final_message['Subject'] = subject
    final_message['From'] = "info@agmtechnology.com"
    final_message.attach(message)

    # Attach the logo image
    logo_path = 'public/assets/brand/agm-logo.png'
    with open(logo_path, 'rb') as logo_file:
        logo_mime = MIMEImage(logo_file.read())
        logo_mime.add_header('Content-ID', '<logo>')
        final_message.attach(logo_mime)

    logger.success(f'Successfully created HTML email with subject: {subject}')
    return jsonify(final_message)

  @handle_exception
  def send_client_email(self, plain_text, client_email, subject):
    logger.info(f'Sending client email to: {client_email}')
    response = self.create_html_email(plain_text, subject)

    message = response['content']
    message['To'] = client_email
    message['Bcc'] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com"

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    create_message = {"raw": raw_message}

    send_message = (
        self.service.users()
        .messages()
        .send(userId="me", body=create_message)
        .execute()
    )
    logger.success(f'Successfully sent client email to: {client_email}')
    return jsonify({'emailId': send_message["id"]})