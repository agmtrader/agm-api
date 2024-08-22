#make a POST request
import requests as rq
import pandas as pd
from io import BytesIO

debug = True
if debug:
    url = 'http://127.0.0.1:5000'
else:
    url = 'https://laserfocus-api.onrender.com'

message = 'Enter a choice or press enter to exit: '
print(
"""
1. Fetch reports
"""
)
choice = input(message)
print('\n')

match choice:
    case '1':
        flex_query_ids = input('Enter Flex Query Ids separated by commas: ')
        flex_query_ids = flex_query_ids.split(',')
        dictToSend = {'queryIds': flex_query_ids}
        response = rq.post(url + '/fetchReports', json=dictToSend)
        print('Fetching reports. Please wait 1 minute.')
        response = response.json()
    case '':
        print('Exiting...')
    case _:
        print('Invalid choice')

print('API Response:', response)