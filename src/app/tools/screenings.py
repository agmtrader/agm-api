from flask import Blueprint, request

from src.components.tools.screenings import run_screenings
from src.utils.response import format_response

bp = Blueprint('screenings', __name__)


@bp.route('/run', methods=['GET'])
@format_response
def run_screenings_route():
    apply_screenings = request.args.get('apply_screenings', 'true').lower() == 'true'
    return run_screenings(apply_screenings=apply_screenings)
