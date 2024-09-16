from flask import Flask, render_template
from flask_cors import CORS

def start_api():
    
    app = Flask(__name__)
    cors = CORS(app, resources={r"/*": {"origins": "*"}})
    app.config['CORS_HEADERS'] = 'Content-Type'

    from app.routes import reporting, trades
    app.register_blueprint(reporting.bp)
    app.register_blueprint(trades.bp)
    
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