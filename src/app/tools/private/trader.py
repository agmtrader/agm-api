import base64
import re
from email.message import EmailMessage
from email.utils import formataddr

from flask import Blueprint, request

from src.utils.authz import TRADER_LOGIN_CODE_SCOPE, require_scope
from src.utils.connectors.gmail import GmailConnector
from src.utils.exception import ServiceError
from src.utils.response import format_response


bp = Blueprint("trader", __name__)
CODE_PATTERN = re.compile(r"[0-9]{8}")
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9.!#$%&'*+/=?^`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")


def valid_recipient(address):
    if not isinstance(address, str) or len(address) > 254 or not EMAIL_PATTERN.fullmatch(address):
        return False
    local, domain = address.split("@")
    return (
        len(local) <= 64
        and not local.startswith(".")
        and not local.endswith(".")
        and ".." not in local
        and all(
            len(label) <= 63 and not label.startswith("-") and not label.endswith("-")
            for label in domain.split(".")
        )
    )


@bp.post("/send_login_code")
@require_scope(TRADER_LOGIN_CODE_SCOPE)
@format_response
def send_login_code():
    """Deliver only the fixed Trader sign-in message, with one recipient."""
    request.max_content_length = 1024
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or payload.keys() != {"email", "code"}:
        raise ServiceError("Invalid login-code request", status_code=400)

    address, code = payload["email"], payload["code"]
    if not valid_recipient(address) or not isinstance(code, str) or not CODE_PATTERN.fullmatch(code):
        raise ServiceError("Invalid login-code request", status_code=400)

    message = EmailMessage()
    message["From"] = formataddr(("AGM Trader", "info@agmtechnology.com"))
    message["To"] = address
    message["Subject"] = "Tu código para iniciar sesión en AGM Trader"
    message.set_content(
        f"Tu código de inicio de sesión es {code}.\n\n"
        "El código vence en 10 minutos y solo se puede usar una vez.\n\n"
        "Si no solicitaste un código, podés ignorar este mensaje."
    )
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
    try:
        response = GmailConnector().service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()
    except Exception:
        raise ServiceError("Login email delivery failed", status_code=502) from None
    if not isinstance(response, dict) or not response.get("id"):
        raise ServiceError("Login email delivery failed", status_code=502)
    return {"accepted": True}
