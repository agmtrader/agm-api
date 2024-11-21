import pytz
from datetime import datetime

def getCurrentCST():
    cst = pytz.timezone('America/Costa_Rica')
    cst_time = datetime.now(cst)
    return cst_time