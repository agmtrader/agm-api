from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager, verify_jwt_in_request, create_access_token, exceptions
import os
from dotenv import load_dotenv
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from src.utils.logger import logger

load_dotenv()

public_routes = ['docs', 'index', 'login']

# JWT authentication middleware
def jwt_required_except_login():
    if request.endpoint not in public_routes:
        try:
            verify_jwt_in_request()
        except exceptions.JWTExtendedException as e:
            return jsonify({"msg": str(e)}), 401
        
def start_api():
    
    app = Flask(__name__, static_folder='static')
    cors = CORS(app, resources={r"/*": {"origins": "*"}})
    app.config['CORS_HEADERS'] = 'Content-Type'
    
    # Add JWT configuration
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
    jwt = JWTManager(app)

    # Initialize Limiter
    limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=["60 per minute"],
        storage_uri='memory://'
    )

    # Apply JWT authentication to all routes except login
    app.before_request(jwt_required_except_login)

    from src.app import reporting, drive, database, flex_query, investment_proposals, bonds, advisors, email, trade_tickets
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')
    app.register_blueprint(drive.bp, url_prefix='/drive')
    app.register_blueprint(database.bp, url_prefix='/database')
    app.register_blueprint(flex_query.bp, url_prefix='/flex_query')
    app.register_blueprint(email.bp, url_prefix='/email')
    app.register_blueprint(investment_proposals.bp, url_prefix='/investment_proposals')
    app.register_blueprint(bonds.bp, url_prefix='/bonds')
    app.register_blueprint(advisors.bp, url_prefix='/advisors')

    # Create index route
    @app.route('/')
    def index():
        return send_from_directory('public/static', 'index.html')
    
    # Create documentation pages
    @app.route('/docs')
    def docs():
        return send_from_directory('public/static', 'docs.html')
    
    @app.route('/docs/drive')
    def drive():
        return send_from_directory('public/static/docs', 'drive.html')

    # Create backend routes
    @app.route('/login', methods=['POST'])
    def login():
        logger.info(f'Login request: {request.get_json(force=True)}')
        payload = request.get_json(force=True)
        username = payload['username']
        password = payload['password']
        if username == 'admin' and password == 'password':
            access_token = create_access_token(identity=username)
            return jsonify(access_token=access_token), 200
        return jsonify({"msg": "Bad username or password"}), 401

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
