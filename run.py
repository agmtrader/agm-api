from flask import Flask, jsonify, request, send_from_directory, g
from flask_cors import CORS
from flask_jwt_extended import JWTManager, verify_jwt_in_request, create_access_token, exceptions
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from src.utils.logger import logger
from datetime import timedelta
from src.utils.secret_manager import get_secret

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

    # Apply rate limit decorator to all routes

    # Apply JWT authentication to all routes except login
    app.before_request(jwt_required_except_login)
    
    from src.app import account_management, accounts, advisors, contacts, document_center, email, flex_query, investment_proposals, leads, notifications, reporting, risk_profiles, tickets, trade_tickets, users
    app.register_blueprint(account_management.bp, url_prefix='/account_management')
    app.register_blueprint(accounts.bp, url_prefix='/accounts')
    app.register_blueprint(advisors.bp, url_prefix='/advisors')
    app.register_blueprint(contacts.bp, url_prefix='/contacts')
    app.register_blueprint(document_center.bp, url_prefix='/document_center')
    app.register_blueprint(email.bp, url_prefix='/email')
    app.register_blueprint(flex_query.bp, url_prefix='/flex_query')
    app.register_blueprint(investment_proposals.bp, url_prefix='/investment_proposals')
    app.register_blueprint(leads.bp, url_prefix='/leads')
    app.register_blueprint(notifications.bp, url_prefix='/notifications')
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(risk_profiles.bp, url_prefix='/risk_profiles')
    app.register_blueprint(tickets.bp, url_prefix='/tickets')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')
    app.register_blueprint(users.bp, url_prefix='/users')
    
    # Create index route
    @app.route('/')
    def index():
        return send_from_directory('public/static', 'index.html')
    
    # Create documentation pages
    @app.route('/docs')
    def docs():
        return send_from_directory('public/static', 'docs.html')
    
    # Create backend routes
    from src.components.users import read_user_by_id
    @app.route('/token', methods=['POST'])
    def token():
        logger.announcement('Token request.')
        payload = request.get_json(force=True)
        token = payload['token']
        scopes = payload['scopes']

        expires_delta = DEFAULT_TOKEN_EXPIRES
        user = read_user_by_id(token)

        if user:
            logger.info(f'Generating access token for user {token} with scopes {scopes} and expiration {expires_delta}')
            access_token = create_access_token(
                identity=token,
                additional_claims={"scopes": scopes},
                expires_delta=expires_delta
            )
            logger.announcement(f'Authenticated user {token}', 'success')
            return jsonify(
                access_token=access_token,
                expires_in=int(expires_delta.total_seconds())
            ), 200
        return jsonify({"msg": "Unauthorized"}), 401
    
    from src.app.misc import oauth
    app.register_blueprint(oauth.bp, url_prefix='/oauth')

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

    return app

app = start_api()
logger.announcement('Running safety checks...', type='info')
logger.announcement('Successfully started API', type='success')
