from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager, verify_jwt_in_request, exceptions, create_access_token
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from src.utils.logger import logger
from datetime import timedelta
from src.utils.managers.secret_manager import get_secret

load_dotenv()

public_routes = ['docs', 'index', 'token', 'oauth.login', 'oauth.create', 'yfinance.get_scroller_data']

def jwt_required_except_login():
    logger.info(f'\nRequest endpoint: {request.endpoint}')
    if request.endpoint not in public_routes:
        try:
            verify_jwt_in_request()
        except exceptions.JWTExtendedException as e:
            return jsonify({"msg": str(e)}), 401
 
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

    # Index page
    @app.route('/')
    def index():
        return send_from_directory('public/static', 'index.html')
    
    # Documentation page
    @app.route('/docs')
    def docs():
        return send_from_directory('public/static', 'docs.html')
    
    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({"error": "Internal server error"}), 500 

    @app.errorhandler(400)
    def bad_request_error(error):
        app.logger.error(f'Bad request: {error}')
        return jsonify({"error": "Bad request", "message": str(error)}), 400

    @app.errorhandler(401)
    def unauthorized_error(error):
        app.logger.error(f'Unauthorized access attempt: {error}')
        return jsonify({"error": "Unauthorized", "message": "Authentication required"}), 401

    @app.errorhandler(403)
    def forbidden_error(error):
        app.logger.error(f'Forbidden access attempt: {error}')
        return jsonify({"error": "Forbidden", "message": "You don't have permission to access this resource"}), 403

    # JWT Token
    from src.components.users import read_user_by_id
    @app.route('/token', methods=['POST'])
    def token():
        logger.announcement('Token request.')
        payload = request.get_json(force=True)

        token = payload['token']
        scopes = payload['scopes']

        if token is None or token == '':
            return jsonify({"msg": "Unauthorized"}), 401

        if scopes is None or scopes == '':
            return jsonify({"msg": "Unauthorized"}), 401

        expires_delta = DEFAULT_TOKEN_EXPIRES
        user = read_user_by_id(str(token))

        if user and user is not None and user['id'] == token:
            logger.info(f'Generating access token for user with scopes {scopes}.')
            access_token = create_access_token(
                identity=token,
                additional_claims={"scopes": scopes},
                expires_delta=expires_delta
            )
            logger.announcement(f'Authenticated user', 'success')
            return jsonify(
                access_token=access_token,
                expires_in=int(expires_delta.total_seconds())
            ), 200

        logger.error(f'Failed to authenticate user {token}')
        return jsonify({"msg": "Unauthorized"}), 401
    
    # Ada
    from src.app.ada import gemini
    app.register_blueprint(gemini.bp, url_prefix='/ada')

    # Auth
    from src.app.auth import oauth
    app.register_blueprint(oauth.bp, url_prefix='/oauth')

    # Documents
    #from src.app.documents import client_documents
    #app.register_blueprint(client_documents.bp, url_prefix='/documents/clients')

    # Tools
    #from src.app.tools import email, tws
    #app.register_blueprint(email.bp, url_prefix='/email')
    #app.register_blueprint(tws.bp, url_prefix='/tws')

    # CRUD
    from src.app import accounts, advisors, applications, contacts, leads, reporting, risk_profiles, trade_tickets, users
    app.register_blueprint(accounts.bp, url_prefix='/accounts')
    app.register_blueprint(advisors.bp, url_prefix='/advisors')
    app.register_blueprint(applications.bp, url_prefix='/applications')
    app.register_blueprint(contacts.bp, url_prefix='/contacts')
    app.register_blueprint(leads.bp, url_prefix='/leads')
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(risk_profiles.bp, url_prefix='/risk_profiles')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')
    app.register_blueprint(users.bp, url_prefix='/users')

    return app

app = start_api()
logger.announcement('Running safety checks...', type='info')
logger.announcement('Successfully started AGM API', type='success')
