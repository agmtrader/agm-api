from flask import Blueprint
from src.components.bonds import Bonds

bp = Blueprint('bonds', __name__)
Bonds = Bonds()

@bp.route('/get_bonds', methods=['GET'])
def get_bonds_route():
  return Bonds.get()