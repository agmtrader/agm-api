#make a POST request
import requests as rq
import pandas as pd
from io import BytesIO

def etlInterface():

    print('Generating ETL')

    dictToSend = {}
    res = rq.post(url + '/', json=dictToSend)
    dictFromServer = res.json()
    print('API Response:', dictFromServer)
    return dictFromServer

debug = True
if debug:
    url = 'http://127.0.0.1:5000'
else:
    url = 'https://laserfocus-api.onrender.com'

message = 'Enter a choice or press enter to exit: '
print(
"""
1. ETL
"""
)
choice = input(message)
print('\n')

match choice:
    case '1':
        response = etlInterface()
    case '':
        print('Exiting...')
    case _:
        print('Invalid choice')

print(response)