from flask import Flask, request
from flask_cors import CORS

import pandas as pd

from agm import AGM

import requests as rq

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

AGM = AGM()

# Reports

@app.route('/fetchReports', methods=['POST'])
async def fetchReports():
    body = request.get_json(force=True)
    response = AGM.fetchReports(queryIds=body['queryIds'])
    return response

# Trade Ticket
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

# Drive
@app.route('/getSharedDriveInfo', methods=['POST'])
async def getSharedDriveInfo():
    body = request.get_json(force=True)
    driveName = body['driveName']
    response = AGM.Drive.getSharedDriveInfo(driveName)
    return response

@app.route('/getFolderInfo', methods=['POST'])
async def getFolderInfo():
    body = request.get_json(force=True)
    parentId = body['parentId']
    folderName = body['folderName']
    response = AGM.Drive.getFolderInfo(parentId, folderName)
    return response

@app.route('/uploadCSVFiles', methods=['POST'])
async def uploadCSVFiles():
    body = request.get_json(force=True)
    files = body['files']
    parentId = body['parentId']
    response = AGM.Drive.uploadCSVFiles(files, parentId)
    return response

# Database
@app.route('/getDocumentsFromCollection', methods=['POST'])
async def getDocumentsFromCollection():
    body = request.get_json(force=True)
    path = body['path']
    response = AGM.Database.getDocumentsFromCollection(path)
    return response

@app.route('/queryDocumentsFromCollection', methods=['POST'])
async def queryDocumentsFromCollection():
    body = request.get_json(force=True)
    path = body['path']
    key = body['key']
    value = body['value']
    response = AGM.Database.queryDocumentsFromCollection(path, key, value)
    return response


if __name__ == '__main__':
    debug = True
    app.run(debug=debug)