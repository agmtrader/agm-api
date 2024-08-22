from flask import Flask, request
from flask_cors import CORS

import pandas as pd

from agm import AGM

import requests as rq

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

AGM = AGM()

@app.route('/fetchReports', methods=['POST'])
async def fetchReports():
    body = request.get_json(force=True)
    response = AGM.fetchReports(queryIds=body['queryIds'])
    return response

debug = True
if debug:

    if __name__ == "__main__": 
        app.run(debug=True)
        
    print('Service live.')