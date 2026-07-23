import time
import uuid

from flask import Flask, g, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager, verify_jwt_in_request, exceptions, create_access_token
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from src.utils.logger import logger
from datetime import timedelta
from src.utils.managers.secret_manager import get_secret
from src.utils.response import format_response
from src.utils.exception import (
    ServiceError,
    build_error_payload,
    log_service_error,
    wrap_unhandled_exception,
)

load_dotenv()

public_routes = ['docs', 'index', 'token', 'users.login', 'users.create']

def jwt_required_except_login():
    if request.endpoint not in public_routes:
        try:
            verify_jwt_in_request()
        except exceptions.JWTExtendedException as e:
            return jsonify({"msg": "Unauthorized"}), 401
 
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
    DEFAULT_TOKEN_EXPIRES = timedelta(hours=1)
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = DEFAULT_TOKEN_EXPIRES
    jwt = JWTManager(app)

    # Initialize Limiter
    limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=["600 per minute"],
        storage_uri='memory://',
        strategy="fixed-window"
    )

    # Apply JWT authentication to all routes except login
    app.before_request(jwt_required_except_login)

    @app.before_request
    def attach_request_context():
        g.request_id = request.headers.get('X-Request-ID', uuid.uuid4().hex[:12])
        g.request_started_at = time.perf_counter()

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
    @format_response
    def token():
        """Generate a short-lived API access token for the special local token payload."""
        logger.announcement('Token request.')
        payload = request.get_json(force=True)

        token_value = payload.get('token')

        if not token_value:
            logger.error('Token is missing')
            raise ServiceError("Unauthorized", status_code=401)

        expires_delta = DEFAULT_TOKEN_EXPIRES

        logger.info(f'Generating access token for user.')
        if token_value == 'all':
            access_token = create_access_token(
                identity=token_value,
                expires_delta=expires_delta
            )
            logger.announcement('Authenticated user', 'success')
            return {
                "access_token": access_token,
                "expires_in": int(expires_delta.total_seconds())
            }

        logger.error(f'Failed to authenticate user.')
        raise ServiceError("Unauthorized", status_code=401)

    # Tools
    from src.app.tools.private import actions, etl
    app.register_blueprint(actions.bp, url_prefix='/actions')
    app.register_blueprint(etl.bp, url_prefix='/etl')

    from src.app.tools.public import email, reporting, trade_tickets
    app.register_blueprint(email.bp, url_prefix='/email')
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')

    # Clients
    from src.app.clients import accounts, account_contacts, advisors, applications, contacts, documents, investment_proposals, risk_profiles, users
    app.register_blueprint(accounts.bp, url_prefix='/accounts')
    app.register_blueprint(account_contacts.bp, url_prefix='/account_contacts')
    app.register_blueprint(advisors.bp, url_prefix='/advisors')
    app.register_blueprint(applications.bp, url_prefix='/applications')
    app.register_blueprint(contacts.bp, url_prefix='/contacts')
    app.register_blueprint(documents.bp, url_prefix='/documents')
    app.register_blueprint(investment_proposals.bp, url_prefix='/investment_proposals')
    app.register_blueprint(risk_profiles.bp, url_prefix='/risk_profiles')
    app.register_blueprint(users.bp, url_prefix='/users')

    from src.app.clients import management_type_requests, advisor_changes, fee_template_requests, flagged_deposits, document_review_emails, document_review_responsibles
    app.register_blueprint(fee_template_requests.bp, url_prefix='/fee_template_requests')
    app.register_blueprint(flagged_deposits.bp, url_prefix='/flagged_deposits')
    app.register_blueprint(advisor_changes.bp, url_prefix='/advisor_changes')
    app.register_blueprint(management_type_requests.bp, url_prefix='/management_type_requests')
    app.register_blueprint(document_review_emails.bp, url_prefix='/document_review_emails')
    app.register_blueprint(document_review_responsibles.bp, url_prefix='/document_review_responsibles')

    
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
