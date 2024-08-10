from flask import Flask
from flask_cors import CORS

import pandas as pd

from agm import AGM

import requests as rq

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

AGM = AGM()

@app.route('/', methods=['POST'])
async def fetchReports():
    response = AGM.runETL()
    return response

debug = True
if debug:

    if __name__ == "__main__": 
        app.run(debug=True)
        
    print('Service live.')