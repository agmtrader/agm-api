from src.utils.connectors.gmail import GmailConnector
from src.utils.exception import handle_exception

# Remove low-level Gmail implementation details; delegate to connector
class Gmail(GmailConnector):
    """High-level email service containing business-specific helper methods.
    Inherits low-level send_email / create_html_email from GmailConnector.
    """

    # ------------------------------------------------------------------
    # Business convenience wrappers
    # ------------------------------------------------------------------

    @handle_exception
    def send_email_confirmation(self, content, client_email, lang="es"):
        subject = (
            "Confirmación de Correo Electrónico" if lang == "es" else "Email Confirmation"
        )
        email_template = f"application_email_confirmation_{lang}"
        return self.send_email(content, client_email, subject, email_template, bcc="", cc="")

    @handle_exception
    def send_trade_ticket_email(self, content, client_email):
        subject = "Confirmación de Transacción"
        email_template = "trade_ticket"
        return self.send_email(content, client_email, subject, email_template)

    @handle_exception
    def send_application_link_email(self, content, client_email, lang="es"):
        subject = (
            "Link de formulario para apertura de cuenta"
            if lang == "es"
            else "Application Link"
        )
        email_template = f"application_link_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com",
        )

    @handle_exception
    def send_task_reminder_email(self, content, agm_user_email):
        subject = "Task Reminder"
        email_template = "task_reminder"
        return self.send_email(content, agm_user_email, subject, email_template, bcc="", cc="")

    @handle_exception
    def send_lead_reminder_email(self, content, agm_user_email):
        subject = "Lead Reminder"
        email_template = "lead_reminder"
        return self.send_email(content, agm_user_email, subject, email_template, bcc="", cc="")

    @handle_exception
    def send_credentials_email(self, content, client_email, lang="es"):
        subject = (
            "Credenciales de acceso para cuenta AGM"
            if lang == "es"
            else "Access Credentials for AGM Account"
        )
        email_template = f"credentials_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com",
        )

    @handle_exception
    def send_transfer_instructions_email(self, content, client_email, lang="es"):
        subject = "Instrucciones de transferencia" if lang == "es" else "Transfer Instructions"
        email_template = f"transfer_instructions_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com",
        )

    @handle_exception
    def send_welcome_email(self, content, client_email, lang="es"):
        """Send welcome email after account approval and funding."""
        subject = (
            "Bienvenido a AGM Technology" if lang == "es" else "Welcome to AGM Technology"
        )
        email_template = f"welcome_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com",
        )