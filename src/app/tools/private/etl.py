from flask import Blueprint, request
from src.components.tools.private.etl import run_pipeline
from src.utils.response import format_response

bp = Blueprint('etl', __name__)

@bp.route('/clients', methods=['GET'])
@format_response
def run_clients_pipeline_route():
    """Run the clients ETL pipeline."""
    return run_pipeline('clients')

@bp.route('/market_data', methods=['GET', 'POST'])
@format_response
def run_market_data_pipeline_route():
    """Dispatch the market job, or poll its operation; never run ETL in Flask."""
    from src.jobs.market_dispatch import dispatch, poll
    if request.method == 'GET' and 'operation' in request.args:
        return poll(request.args['operation']), 200, {'Cache-Control': 'no-store'}
    body, status = dispatch()
    return body, status, {'Cache-Control': 'no-store'}
