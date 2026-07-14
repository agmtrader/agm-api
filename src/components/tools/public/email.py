from src.utils.connectors.gmail import GmailConnector
from src.utils.exception import ServiceError, handle_exception

class Gmail(GmailConnector):
    """High-level email service containing business-specific helper methods.
    Inherits low-level send_email / create_html_email from GmailConnector.
    """
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
    def send_credentials_email(self, content, client_email, lang="es", cc=""):
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
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com," + cc,
        )

    @handle_exception
    def send_transfer_instructions_email(self, content, client_email, lang="es", cc="", initial=True):
        subject = "Instrucciones de transferencia" if lang == "es" else "Transfer Instructions"
        
        template_name = "transfer_instructions"
        if not initial:
            template_name = "transfer_instructions_existing"
            
        email_template = f"{template_name}_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com," + cc,
        )

    @handle_exception
    def send_welcome_email(self, content, client_email, lang="es", cc=""):
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
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com," + cc,
        )

    @handle_exception
    def send_funding_notification_email(self, content, client_email, lang="es", cc="", days_since_opened=None, notice_number=None):
        """Send funding notification email."""
        calculated_notice_number = 1
        if notice_number is not None:
            try:
                calculated_notice_number = max(1, int(notice_number))
            except (TypeError, ValueError):
                calculated_notice_number = 1
        elif days_since_opened is not None:
            try:
                # One notice per business week (5 business days), rounded up.
                calculated_notice_number = max(1, int((int(days_since_opened) + 4) // 5))
            except (TypeError, ValueError):
                calculated_notice_number = 1

        subject = (
            f"Recordatorio de Fondeo"
            if lang == "es"
            else f"Funding Reminder"
        )
        email_template = f"funding_notification_{lang}"
        return self.send_email(
            content,
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com," + cc,
        )

    @handle_exception
    def send_missing_documents_email(self, content, client_email, missing_type="multiple", lang="en", cc=""):
        normalized_lang = "es" if lang == "es" else "en"
        normalized_missing_type = missing_type if missing_type in {"poi", "poa", "sow", "multiple"} else "multiple"
        is_company_contact = bool((content or {}).get("is_company_contact"))
        company_name = str((content or {}).get("company_name") or "").strip()
        if is_company_contact and not company_name:
            raise ServiceError(
                "company_name is required for company missing-documents emails",
                status_code=400,
            )
        if normalized_lang == "es":
            subject = (
                "Documentos pendientes para su cuenta corporativa"
                if is_company_contact
                else "Documentos pendientes para su cuenta personal"
            )
        else:
            subject = (
                "Pending Documents for Your Corporate Account"
                if is_company_contact
                else "Pending Documents for Your Personal Account"
            )
        email_template = f"missing_documents_{normalized_lang}"

        return self.send_email(
            {
                **(content or {}),
                "company_name": company_name,
                "missing_type": normalized_missing_type,
            },
            client_email,
            subject,
            email_template,
            bcc="",
            cc="jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com," + cc,
        )

    @handle_exception
    def send_compliance_manual_update_email(self, content, recipient_email="aa@agmtechnology.com"):
        subject = "Compliance Manual Update Requires Review"
        email_template = "compliance_manual_update"
        return self.send_email(
            content or {},
            recipient_email,
            subject,
            email_template,
            bcc="",
            cc="",
        )
