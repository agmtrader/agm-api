#make a POST request
import requests as rq
import pandas as pd
from io import BytesIO

debug = True
if debug:
    url = 'http://127.0.0.1:5000'

message = 'Enter a choice or press enter to exit: '
print(
"""
1. Generate ACOBO trade ticket
"""
)
choice = input(message)
print('\n')

match choice:
    case '1':
        response = rq.post(url + '/fetchReports', json={'queryId':['986431']})
        print('Fetching reports. Please wait 1 minute.')
        tradeTicket = response.json()[0]

        tradeData = rq.post(url + '/processTradeTicket', json={'indices':'0', 'tradeTicket':tradeTicket})
        tradeData = tradeData.json()
        print(tradeData)

        response = rq.post(url + '/sendTradeTicketEmail', json={'tradeData':tradeData})


    case '':
        print('Exiting...')
    case _:
        print('Invalid choice')

print('API Response:', response)