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
    response = AGM.fetchReports(queryIds=body['queryId'])
    return response

@app.route('/processTradeTicket', methods=['POST'])
async def processTradeTicket():
    body = request.get_json(force=True)
    indices = body['indices'].split(',')
    indices = [int(index) for index in indices]
    response = AGM.processTradeTicket(indices=indices, flex_query=body['tradeTicket'])
    return response

@app.route('/generateTradeTicketEmail', methods=['POST'])
async def generateTradeTicketEmail():
    body = request.get_json(force=True)
    tradeData = body['tradeData']
    response = AGM.generateTradeTicketEmail(tradeData=tradeData)
    return response

@app.route('/sendClientEmail', methods=['POST'])
async def sendClientEmail():
    body = request.get_json(force=True)
    message = body['message']
    clientEmail = body['clientEmail']
    subject = body['subject']
    response = AGM.Email.sendClientEmail(message, clientEmail, subject)
    return response

debug = True
if debug:

    if __name__ == "__main__": 
        app.run(debug=True)
        
    print('Service live.')