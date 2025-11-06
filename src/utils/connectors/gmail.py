from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.utils import formataddr
from jinja2 import Environment, FileSystemLoader
from premailer import transform

import os
import base64
import time
from functools import wraps
from google.auth.exceptions import RefreshError

from src.utils.logger import logger
from src.utils.exception import handle_exception
from src.utils.managers.secret_manager import get_secret

# ---------------------------------------------------------------------
# Retry decorator (mirrors Drive connector logic)
# ---------------------------------------------------------------------


def retry_on_connection_error(max_retries: int = 3, delay: int = 1):
    """Decorator to retry operations on connection errors, refreshing the Gmail
    service if necessary.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    # Ensure we have a fresh connection before each attempt
                    self._ensure_fresh_connection()
                    return func(self, *args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    error_msg = str(exc).lower()

                    # Identify common connection-related error substrings
                    if any(err in error_msg for err in [
                        "broken pipe",
                        "connection reset",
                        "connection aborted",
                        "connection timeout",
                        "socket.error",
                        "httplib.badstatusline",
                        "ssl.sslerror",
                        "connectionerror",
                        "timeout",
                    ]):
                        logger.warning(
                            f"Gmail connection error on attempt {attempt + 1}/{max_retries}: {exc}"
                        )
                        if attempt < max_retries - 1:
                            # Force a connection refresh and apply exponential back-off
                            self._force_connection_refresh()
                            time.sleep(delay * (attempt + 1))
                            continue

                    # Not a connection error or retries exhausted – re-raise
                    raise exc

            # All retries failed
            raise last_exception

        return wrapper

    return decorator


class GmailConnector:
    """Low-level Gmail connector responsible for authorizing against the Gmail API
    and performing raw email delivery.
    The higher-level components.services.email module should contain the
    business-specific helper methods (e.g. send_trade_ticket_email).
    """

    def __init__(self):
        logger.announcement("Initializing Gmail connection.", type="info")

        # Connection lifecycle tracking
        self._service = None
        self._last_connection_time: float | None = None
        self._connection_timeout: int = 300  # 5 minutes

        # Establish first connection eagerly so that failures surface early
        try:
            self._service = self._create_service()
            logger.announcement("Initialized Gmail connection.", type="success")
        except Exception as exc:
            logger.error(f"Error initializing Gmail: {exc}")
            raise

    # Expose service property (read-only)
    @property
    def service(self):
        return self._service

    # ------------------------------------------------------------------
    # Connection helpers (largely mirrored from Drive connector)
    # ------------------------------------------------------------------

    def _get_credentials(self):
        """Obtain fresh OAuth credentials, refreshing if necessary."""
        creds_data = get_secret("OAUTH_PYTHON_CREDENTIALS_INFO")
        SCOPES = ["https://mail.google.com/"]

        creds = Credentials(
            token=creds_data["token"],
            refresh_token=creds_data["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=creds_data["client_id"],
            client_secret=creds_data["client_secret"],
            scopes=SCOPES,
        )

        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(creds._request)
                logger.info("OAuth token refreshed successfully for Gmail")
            except RefreshError as exc:
                logger.error(f"Failed to refresh Gmail OAuth token: {exc}")
                raise Exception(f"OAuth token refresh failed: {exc}")

        return creds

    def _create_service(self):
        """Create a fresh Gmail service binding."""
        creds = self._get_credentials()
        service = build("gmail", "v1", credentials=creds)
        self._last_connection_time = time.time()
        return service

    def _is_connection_stale(self):
        if self._service is None or self._last_connection_time is None:
            return True
        return (time.time() - self._last_connection_time) > self._connection_timeout

    def _ensure_fresh_connection(self):
        if self._is_connection_stale():
            logger.info("Refreshing Gmail connection…")
            self._service = self._create_service()

    def _force_connection_refresh(self):
        logger.info("Force-refreshing Gmail connection…")
        self._service = None
        self._last_connection_time = None

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    def _render_template(self, email_template: str, template_context: dict) -> str:
        """Render the Jinja template located in src/lib/email_templates."""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            templates_dir = os.path.join(current_dir, "../../lib/email_templates")
            env = Environment(loader=FileSystemLoader(templates_dir))
            template = env.get_template(f"{email_template}.html")
        except Exception as exc:
            logger.error(f"Template {email_template}.html not found – {exc}")
            raise
        return template.render(**template_context)

    def _inline_css(self, html: str) -> str:
        """Inline <style> blocks so that they are supported by most email clients."""
        return transform(html)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def create_html_email(
        self,
        content,
        subject: str,
        client_email: str,
        email_template: str,
        bcc: str = "",
        cc: str = "",
    ) -> dict:
        """Create a raw gmail API payload ready to be sent."""
        logger.info(f"Creating {email_template} email with subject: {subject}")

        # Render + inline
        html_content = self._render_template(
            email_template,
            {
                "subject": subject,
                "content": content,
            },
        )
        html_content_inlined = self._inline_css(html_content)

        # Build MIME tree (multipart/related > multipart/alternative > text & html)
        alt_part = MIMEMultipart("alternative")
        alt_part["Subject"] = subject
        formatted_from = formataddr(("AGM Technology", "info@agmtechnology.com"))
        alt_part["From"] = formatted_from

        if isinstance(content, dict):
            text_content = "\n".join(f"{k}: {v}" for k, v in content.items())
            alt_part.attach(MIMEText(text_content.encode("utf-8"), "plain", "utf-8"))
        alt_part.attach(MIMEText(html_content_inlined.encode("utf-8"), "html", "utf-8"))

        root_message = MIMEMultipart("related")
        root_message["Subject"] = subject
        root_message["To"] = client_email
        root_message["From"] = formatted_from
        root_message["Bcc"] = bcc
        root_message["Cc"] = cc
        root_message.attach(alt_part)

        # Embed logo
        logo_path = "public/assets/brand/agm-logo.png"
        try:
            with open(logo_path, "rb") as fh:
                logo_mime = MIMEImage(fh.read())
                logo_mime.add_header("Content-ID", "<logo>")
                root_message.attach(logo_mime)
        except FileNotFoundError:
            logger.warning(f"Logo not found at {logo_path}. Continuing without it.")

        logger.success(f"Successfully created {email_template} email with subject: {subject}")
        raw = base64.urlsafe_b64encode(root_message.as_bytes()).decode()
        return {"raw": raw}

    # ------------------------------------------------------------------
    @retry_on_connection_error()
    @handle_exception
    def send_email(
        self,
        content,
        client_email: str,
        subject: str,
        email_template: str,
        *,
        bcc: str = "aa@agmtechnology.com,cr@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com",
        cc: str = "",
    ) -> str:
        """Send an email and return Gmail message id."""

        logger.announcement(
            f"Sending {email_template} email to: {client_email}", type="info"
        )

        raw_message = self.create_html_email(
            content, subject, client_email, email_template, bcc, cc
        )

        sent_response = (
            self.service.users().messages().send(userId="me", body=raw_message).execute()
        )

        logger.announcement(
            f"Successfully sent {email_template} email to: {client_email}", type="success"
        )
        return sent_response["id"]
