from flask import Blueprint, request, jsonify
from src.components.tools.gemini import Gemini
from src.utils.logger import logger
from src.utils.response import format_response

bp = Blueprint('gemini', __name__)
gemini = Gemini()

@bp.route('/chat', methods=['POST'])
@format_response
def chat_route():
    payload = request.get_json(force=True)
    messages = payload.get('messages', [])
    try:
        response = gemini.chat(messages)
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error in chat_route: {str(e)}")
        return jsonify({'error': str(e)}), 500