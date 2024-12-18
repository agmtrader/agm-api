from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager, verify_jwt_in_request, create_access_token, exceptions
import os
from dotenv import load_dotenv

from src.app import email, trade_tickets
from src.utils.logger import logger

load_dotenv()

def jwt_required_except_login():
    if request.endpoint != 'login' and request.endpoint != 'index' and request.endpoint != 'docs':
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

    # Apply JWT authentication to all routes except login
    app.before_request(jwt_required_except_login)

    from src.app import reporting, drive, database, flex_query, investment_proposals
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')
    app.register_blueprint(drive.bp, url_prefix='/drive')
    app.register_blueprint(database.bp, url_prefix='/database')
    app.register_blueprint(flex_query.bp, url_prefix='/flex_query')
    app.register_blueprint(email.bp, url_prefix='/email')
    app.register_blueprint(investment_proposals.bp, url_prefix='/investment_proposals')
    
    # Add this route before other routes
    @app.route('/')
    def index():
        return send_from_directory('public/static', 'index.html')
    
    @app.route('/docs')
    def docs():
        return send_from_directory('public/static', 'docs.html')

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
