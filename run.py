import time
import uuid
import os

from flask import Flask, g, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager, create_access_token
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.errors import RateLimitExceeded
from flask_limiter.util import get_remote_address
from src.utils.logger import logger
from datetime import timedelta
from src.utils.managers.secret_manager import get_secret
from src.utils.connectors.supabase import initialize_database
from src.utils.response import format_response
from src.utils.exception import (
    ServiceError,
    build_error_payload,
    log_service_error,
    wrap_unhandled_exception,
)
from src.utils.authz import authenticate_request, enforce_route_scope

load_dotenv()

public_routes = ['docs', 'index', 'token', 'users.login']
 
def start_api():

    try:
        jwt_secret_key = get_secret('JWT_SECRET_KEY')
    except Exception as e:
        logger.error(f"Failed to fetch JWT secret key: {str(e)}")
        raise Exception("Failed to initialize API - could not fetch JWT secret key")
    
    app = Flask(__name__, static_folder='static')
    cors = CORS(app, resources={r"/*": {"origins": "*"}})
    app.config['CORS_HEADERS'] = 'Content-Type'
    
    # Add JWT configuration
    app.config['JWT_SECRET_KEY'] = jwt_secret_key

    # Default expiration time (1 hour)
    # Keep bearer credentials short-lived; callers refresh through their
    # authenticated user credentials when the cache expires.
    DEFAULT_TOKEN_EXPIRES = timedelta(minutes=15)
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = DEFAULT_TOKEN_EXPIRES
    jwt = JWTManager(app)

    # Keep the broad API limit moderate, and make the values configurable so
    # deployments can tune them without changing code.  The default in-memory
    # backend is suitable for a single local process; production deployments
    # should set RATELIMIT_STORAGE_URI to a shared backend such as Redis.
    default_rate_limit = os.getenv('API_RATE_LIMIT', '60 per minute')
    auth_rate_limit = os.getenv('API_AUTH_RATE_LIMIT', '5 per minute')
    token_rate_limit = os.getenv('API_TOKEN_RATE_LIMIT', '3 per minute')
    rate_limit_storage = os.getenv('RATELIMIT_STORAGE_URI', 'memory://')

    # Initialize Limiter
    limiter = Limiter(
        get_remote_address,
        app=app,
        application_limits=[default_rate_limit],
        storage_uri=rate_limit_storage,
        strategy=os.getenv('RATELIMIT_STRATEGY', 'fixed-window'),
        headers_enabled=True,
    )

    # Initialize the database explicitly before importing blueprints. This
    # preserves fail-fast schema validation in production while keeping
    # component imports free of database side effects.
    initialize_database()

    @app.before_request
    def attach_request_context():
        g.request_id = request.headers.get('X-Request-ID', uuid.uuid4().hex[:12])
        g.request_started_at = time.perf_counter()

    # Authenticate every non-public request and enforce its server-side scope.
    # Request context is attached first so auth failures receive the same
    # request ID as the response and audit log.
    app.before_request(authenticate_request)
    app.before_request(enforce_route_scope)

    @app.after_request
    def attach_response_headers(response):
        request_id = getattr(g, 'request_id', None)
        if request_id:
            response.headers['X-Request-ID'] = request_id

        started_at = getattr(g, 'request_started_at', None)
        if started_at is not None:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            if response.status_code >= 500:
                logger.error(
                    f"HTTP {request.method} {request.path} completed with {response.status_code} "
                    f"in {duration_ms}ms [request_id={request_id}]"
                )
            elif response.status_code in (401, 403):
                principal = getattr(g, 'current_principal_id', 'anonymous')
                logger.warning(
                    f"Authorization denied: {request.method} {request.path} -> {response.status_code} "
                    f"[principal={principal}] [request_id={request_id}]"
                )
            elif duration_ms >= 3000:
                logger.warning(
                    f"Slow request: {request.method} {request.path} completed with {response.status_code} "
                    f"in {duration_ms}ms [request_id={request_id}]"
                )

        return response

    # Index page
    @app.route('/')
    def index():
        """Serve the static AGM API landing page."""
        return send_from_directory('public/static', 'index.html')
    
    # Documentation page
    @app.route('/docs')
    def docs():
        """Serve the generated AGM API route documentation page."""
        return send_from_directory('public/static', 'docs.html')
    
    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({"error": "Not found", "error_id": getattr(g, 'request_id', None)}), 404

    @app.errorhandler(500)
    def internal_error(error):
        err = ServiceError(
            message="Internal server error",
            status_code=500,
            code="internal_error",
            error_id=getattr(g, 'request_id', None),
        )
        log_service_error(err, request.endpoint or 'flask.500')
        return jsonify(build_error_payload(err)), 500

    @app.errorhandler(400)
    def bad_request_error(error):
        app.logger.error(f'Bad request: {error}')
        return jsonify({
            "error": "Bad request",
            "message": str(error),
            "error_id": getattr(g, 'request_id', None),
        }), 400

    @app.errorhandler(401)
    def unauthorized_error(error):
        app.logger.error(f'Unauthorized access attempt: {error}')
        return jsonify({
            "error": "Unauthorized",
            "message": "Authentication required",
            "error_id": getattr(g, 'request_id', None),
        }), 401

    @app.errorhandler(403)
    def forbidden_error(error):
        app.logger.error(f'Forbidden access attempt: {error}')
        return jsonify({
            "error": "Forbidden",
            "message": "You don't have permission to access this resource",
            "error_id": getattr(g, 'request_id', None),
        }), 403

    @app.errorhandler(RateLimitExceeded)
    def rate_limit_error(error):
        """Return a stable JSON response when a client exceeds a limit."""
        response = jsonify({
            "error": "Too many requests",
            "message": "Rate limit exceeded. Please retry later.",
            "error_id": getattr(g, 'request_id', None),
        })
        response.status_code = 429
        return response

    @app.errorhandler(ServiceError)
    def service_error(error):
        if error.status_code >= 500:
            log_service_error(error, request.endpoint or 'flask.service_error')
        return jsonify(build_error_payload(error)), error.status_code

    @app.errorhandler(Exception)
    def unexpected_error(error):
        wrapped = wrap_unhandled_exception(error, request.endpoint or 'flask')
        return jsonify(build_error_payload(wrapped)), wrapped.status_code

    # JWT Token
    @app.route('/token', methods=['POST'])
    @limiter.limit(token_rate_limit, override_defaults=True)
    @format_response
    def token():
        """Generate a short-lived token for an existing AGM user.

        The former universal ``{"token":"all"}`` credential is deliberately
        rejected.  Callers must authenticate with an actual AGM user account.
        """
        logger.announcement('Token request.')
        payload = request.get_json(silent=True) or {}

        email = payload.get('email')
        password = payload.get('password')
        if not email or not password:
            raise ServiceError("Unauthorized", status_code=401)

        from src.components.clients.users import read_users, verify_password
        users = read_users(query={'email': email}, include_sensitive=True)
        if len(users) != 1 or not verify_password(password, users[0].get('password_hash')):
            raise ServiceError("Unauthorized", status_code=401)
        user = users[0]
        if user.get('is_active', True) is False:
            raise ServiceError("Unauthorized", status_code=401)

        expires_delta = DEFAULT_TOKEN_EXPIRES
        claims = {}
        if user.get('token_version') is not None:
            claims['token_version'] = user['token_version']
        access_token = create_access_token(
            identity=str(user['id']),
            additional_claims=claims,
            expires_delta=expires_delta,
        )
        return {
            "access_token": access_token,
            "expires_in": int(expires_delta.total_seconds()),
            "user_id": str(user['id']),
        }

    # Tools
    from src.app.tools.private import actions, etl
    app.register_blueprint(actions.bp, url_prefix='/actions')
    app.register_blueprint(etl.bp, url_prefix='/etl')

    from src.app.tools.public import reporting, trade_tickets
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')

    # Clients
    from src.app.clients import accounts, advisors, contacts, investment_proposals, risk_profiles, users, application_providers
    app.register_blueprint(accounts.bp, url_prefix='/accounts')
    app.register_blueprint(advisors.bp, url_prefix='/advisors')
    app.register_blueprint(contacts.bp, url_prefix='/contacts')
    app.register_blueprint(investment_proposals.bp, url_prefix='/investment_proposals')
    app.register_blueprint(risk_profiles.bp, url_prefix='/risk_profiles')
    app.register_blueprint(users.bp, url_prefix='/users')
    app.register_blueprint(application_providers.bp, url_prefix='/application_providers')

    from src.app.clients import management_type_requests, advisor_changes, fee_template_requests, flagged_deposits, document_review_emails, document_review_responsibles
    app.register_blueprint(fee_template_requests.bp, url_prefix='/fee_template_requests')
    app.register_blueprint(flagged_deposits.bp, url_prefix='/flagged_deposits')
    app.register_blueprint(advisor_changes.bp, url_prefix='/advisor_changes')
    app.register_blueprint(management_type_requests.bp, url_prefix='/management_type_requests')
    app.register_blueprint(document_review_emails.bp, url_prefix='/document_review_emails')
    app.register_blueprint(document_review_responsibles.bp, url_prefix='/document_review_responsibles')

    # The login view lives in a blueprint, so apply its stricter public-endpoint
    # limit after the blueprint is registered.  The application-wide limit still
    # applies as a separate safety net.
    login_endpoint = 'users.login'
    login_view = app.view_functions.get(login_endpoint)
    if login_view is not None:
        app.view_functions[login_endpoint] = limiter.limit(
            auth_rate_limit,
            override_defaults=True,
        )(login_view)

    
    return app

app = start_api()
logger.announcement('Running safety checks...', type='info')
logger.announcement('Successfully started AGM API', type='success')

# Generate docs
from src.utils.managers.docs_manager import generate_docs
try:
    generate_docs(app, public_endpoints=public_routes)
    logger.announcement('Documentation generated', type='success')
except Exception as e:
    logger.error(f'Failed to generate docs: {e}')
