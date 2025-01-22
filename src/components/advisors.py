from src.utils.api import access_api
from src.utils.logger import logger
from src.utils.response import Response

def get_commissions():
    response = access_api('/drive/download_file', 'POST', {'file_id': '15tKbOtv3bLX0P6CXNsQ5nyKREyhxSXLR', 'parse': True})
    if response['status'] != 'success':
        raise Exception('Error fetching commissions')
    return Response.success(response['content'])