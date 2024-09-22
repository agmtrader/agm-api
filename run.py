from flask import Flask, render_template
from flask_cors import CORS

from app.routes import email, trade_tickets

def start_api():
    
    app = Flask(__name__)
    cors = CORS(app, resources={r"/*": {"origins": "*"}})
    app.config['CORS_HEADERS'] = 'Content-Type'

    from app.routes import reporting, drive, database, flex_query
    app.register_blueprint(reporting.bp, url_prefix='/reporting')
    app.register_blueprint(trade_tickets.bp, url_prefix='/trade_tickets')
    app.register_blueprint(drive.bp, url_prefix='/drive')
    app.register_blueprint(database.bp, url_prefix='/database')
    app.register_blueprint(flex_query.bp, url_prefix='/flex_query')
    app.register_blueprint(email.bp, url_prefix='/email')

    @app.errorhandler(404)
    def not_found_error(error):
        return {"error": "Not found"}, 404

    @app.errorhandler(500)
    def internal_error(error):
        return {"error": "Internal server error"}, 500 

    return app

if __name__ == '__main__':
    app = start_api()
    debug = True
    app.run(debug=debug, host='0.0.0.0', port=5001)