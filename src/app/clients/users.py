from flask import Blueprint, request, current_app
from flask_jwt_extended import create_access_token
from datetime import timedelta
from src.components.clients.users import read_users, update_user, verify_password, sanitize_user
from src.utils.response import format_response
from src.utils.exception import ServiceError
from src.utils.connectors.supabase import db

bp = Blueprint('users', __name__)

@bp.route('/login', methods=['POST'])
@format_response
def login():
    """Authenticate a user by email and password and return the sanitized user record for valid credentials."""
    payload = request.get_json(silent=True) or {}
    email = payload.get('email')
    password = payload.get('password')
    if not email or not password:
        raise ServiceError('Invalid email or password.', status_code=401)

    users = read_users(query={'email': email}, include_sensitive=True)
    if len(users) == 1:
        user = users[0]
        password_hash = user.get('password_hash')
        if verify_password(password, password_hash):
            expires_delta = current_app.config.get('JWT_ACCESS_TOKEN_EXPIRES', timedelta(hours=8))
            claims = {}
            if user.get('token_version') is not None:
                claims['token_version'] = user['token_version']
            result = sanitize_user(user)
            if 'advisor' in {scope.strip() for scope in str(user.get('scopes') or '').split()}:
                # Resolve the dashboard target once at login. Subsequent
                # requests still enforce ownership in the API.
                user_email = str(user.get('email') or '').casefold()
                contacts = [
                    contact for contact in (db.read(table='contact', query={'email': user.get('email')}) or [])
                    if str(contact.get('email') or '').casefold() == user_email
                ]
                if contacts:
                    contact_id = contacts[0].get('id')
                    advisors = [
                        advisor for advisor in (db.read(table='advisor', query={'contact_id': contact_id}) or [])
                        if str(advisor.get('contact_id')) == str(contact_id)
                    ]
                    if advisors:
                        result['advisor_id'] = str(advisors[0].get('id'))
            result['access_token'] = create_access_token(
                identity=str(user['id']),
                additional_claims=claims,
                expires_delta=expires_delta,
            )
            result['expires_in'] = int(expires_delta.total_seconds())
            return result

    # Do not disclose whether an email exists or whether its password hash is
    # missing.  A single generic response also avoids turning this endpoint
    # into an account-enumeration oracle.
    raise ServiceError('Invalid email or password.', status_code=401)

@bp.route('/read', methods=['GET'])
@format_response
def read_users_route():
    """Read users from the database, optionally filtered by internal id or user_id."""
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    return read_users(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_user_route():
    """Update a user record selected by id or email without allowing password_hash updates through this route."""
    payload = request.get_json(force=True)
    user = payload.get('user', None)
    if user:
        user.pop('password_hash', None)
    query = {}
    id = payload.get('id', None)
    email = payload.get('email', None)
    if id:
        query['id'] = id
    if email:
        query['email'] = email
    return update_user(query=query, user=user)
