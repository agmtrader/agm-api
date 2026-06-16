from flask import Blueprint
from src.components.tools.private.etl import run_pipeline
from src.utils.response import format_response

bp = Blueprint('etl', __name__)

@bp.route('/clients', methods=['GET'])
@format_response
def run_clients_pipeline_route():
    """Run the clients ETL pipeline."""
    return run_pipeline('clients')

@bp.route('/market_data', methods=['GET'])
@format_response
def run_market_data_pipeline_route():
    """Run the market data ETL pipeline."""
    return run_pipeline('market_data')
