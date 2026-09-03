"""Request authentication and authorization helpers.

The API is the authoritative enforcement point.  Client-side visibility and
caller-supplied user IDs/scopes are never sufficient authorization evidence.
"""

from __future__ import annotations

from functools import wraps
from typing import Iterable

from flask import g, jsonify, request
from flask_jwt_extended import get_jwt, get_jwt_identity, verify_jwt_in_request

from src.utils.connectors.supabase import db
from src.utils.logger import logger


PUBLIC_ENDPOINTS = {"index", "docs", "token", "users.login", "application_providers.read_route"}


def _authorization_denied(status: int, message: str, reason: str, required_scope: str | None = None):
    """Return a client-safe denial and emit a searchable security event."""
    request_id = getattr(g, "request_id", None)
    principal = getattr(g, "current_principal_id", "anonymous")
    scope_text = f" [required_scope={required_scope}]" if required_scope else ""
    logger.error(
        f"AUTHORIZATION_DENIED status={status} method={request.method} path={request.path} "
        f"principal={principal} reason={reason}{scope_text} request_id={request_id}"
    )
    g.authorization_denial_logged = True
    return jsonify({"error": "Unauthorized" if status == 401 else "Forbidden", "message": message}), status


def _normalise_scopes(value: object) -> set[str]:
    if not isinstance(value, str):
        return set()
    return {item.strip() for item in value.split() if item.strip()}


def _load_user(identity: object) -> dict | None:
    if not identity or identity == "all":
        return None
    try:
        users = db.read(table="user", query={"id": str(identity)})
    except Exception:
        # Authentication must fail closed if the identity store is unavailable
        # or the token contains a malformed identifier.
        return None
    if not users or len(users) != 1:
        return None
    user = users[0]
    if user.get("is_active", True) is False:
        return None
    return user


def authenticate_request() -> tuple[dict | None, tuple] | None:
    """Authenticate the request and attach the current AGM user to ``g``.

    Returns a Flask response tuple for an authentication failure, otherwise
    returns ``None`` so it can be used from ``before_request``.
    """

    if request.endpoint in PUBLIC_ENDPOINTS:
        return None

    try:
        verify_jwt_in_request()
    except Exception as exc:
        return _authorization_denied(401, "Authentication required", f"jwt_verification_failed:{type(exc).__name__}")

    claims = get_jwt()
    user = _load_user(get_jwt_identity())
    if user is None:
        return _authorization_denied(401, "Unknown or inactive user", "unknown_or_inactive_user")

    token_version = claims.get("token_version")
    current_version = user.get("token_version")
    if token_version is not None and current_version is not None and token_version != current_version:
        return _authorization_denied(401, "Token revoked", "token_revoked")

    g.current_user = user
    g.current_user_scopes = _normalise_scopes(user.get("scopes"))
    g.current_principal_id = str(user.get("id"))
    return None


def has_scope(required_scope: str | Iterable[str]) -> bool:
    scopes = getattr(g, "current_user_scopes", set())
    required = {required_scope} if isinstance(required_scope, str) else set(required_scope)
    if "all" in scopes or scopes.intersection(required):
        return True
    # Preserve compatibility with the existing coarse scopes (for example
    # ``trade_tickets``) while new users can receive exact route scopes such
    # as ``accounts/read``.  A resource-only scope grants only that resource,
    # never unrelated routes.
    for item in required:
        if "/" in item and item.split("/", 1)[0] in scopes:
            return True
    # The existing database uses the singular ``advisor`` scope while the
    # REST resource is named ``advisors``.
    if any(item.split("/", 1)[0] == "advisors" and "advisor" in scopes for item in required):
        # Advisor Hub reads must be bound to the advisor's own contact.  A
        # missing or unrelated contact_id is denied (prevents BOLA).
        requested_contact_id = request.args.get("contact_id")
        requested_advisor_id = request.args.get("id")
        current_email = (getattr(g, "current_user", {}) or {}).get("email")
        if not (requested_contact_id or requested_advisor_id) or not current_email:
            return False
        try:
            contacts = db.read(table="contact", query={"email": current_email})
            own_contact_ids = {str(contact.get("id")) for contact in contacts or []}
            if requested_contact_id:
                return str(requested_contact_id) in own_contact_ids
            advisors = db.read(table="advisor", query={"contact_id": next(iter(own_contact_ids), "")}) or []
            return any(str(advisor.get("id")) == str(requested_advisor_id) for advisor in advisors)
        except Exception:
            return False

    # Advisor users may resolve only their own contact as part of the Advisor
    # Hub redirect.  This is intentionally narrower than granting contacts/*.
    if "advisor" in scopes and any(scope in required for scope in {"contacts/read", "contacts/documents"}):
        # Advisor Hub may read its own supporting documents, but cannot
        # upload, edit, or delete documents through the generic endpoint.
        if "contacts/documents" in required and request.method != "GET":
            return False
        requested_email = request.args.get("email")
        current_email = (getattr(g, "current_user", {}) or {}).get("email")
        requested_contact_id = request.args.get("contact_id")
        if requested_email and current_email and requested_email.casefold() == current_email.casefold():
            return True
        if requested_contact_id and current_email:
            try:
                contacts = db.read(table="contact", query={"email": current_email}) or []
                return any(str(contact.get("id")) == str(requested_contact_id) for contact in contacts)
            except Exception:
                return False
    return False


def require_scope(required_scope: str | Iterable[str]):
    """Decorator for routes whose permission is not derivable from the URL."""

    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not has_scope(required_scope):
                return _authorization_denied(403, "Insufficient scope", "missing_scope", str(required_scope))
            return view(*args, **kwargs)

        return wrapped

    return decorator


def required_scope_for_path(path: str, method: str) -> str | None:
    """Derive a conservative route scope from the public API path.

    Explicit decorators can override this for exceptional routes.  Nested
    paths retain their operation (for example ``accounts/ibkr/withdraw``),
    preventing a broad ``accounts/read`` permission from authorizing a
    financial side effect.
    """

    parts = [part for part in path.split("/") if part]
    if not parts:
        return None
    if parts[0] in {"reporting", "actions", "etl", "trade_tickets"}:
        return "/".join(parts)
    if len(parts) < 2:
        return None
    # Preserve the complete nested operation.  `/accounts/ibkr/forms` must
    # require `accounts/ibkr/forms`, not the unrelated parent `accounts/ibkr`.
    return "/".join(parts)


def enforce_route_scope():
    """Apply default-deny scope enforcement after authentication."""

    if request.endpoint in PUBLIC_ENDPOINTS:
        return None
    required = required_scope_for_path(request.path, request.method)
    if required is None or has_scope(required):
        return None
    return _authorization_denied(403, f"Scope required: {required}", "missing_scope", required)
