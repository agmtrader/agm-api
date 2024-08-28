#make a POST request
import requests as rq

debug = True
if debug:
    url = 'http://127.0.0.1:5000'

message = 'Enter a choice or press enter to exit: '
print(
"""
1. Generate ACOBO trade ticket
2. Get documents from collection
3. Query documents from collection
4. Generate reports
"""
)
choice = input(message)
print('\n')

match choice:

    case '1':

        response = rq.post(url + '/fetchReports', json={'queryIds':['986431']})
        print('Fetching reports. Please wait 1 minute.')
        tradeTicket = response.json()[0]

        response = rq.post(url + '/processTradeTicket', json={'indices':'0', 'tradeTicket':tradeTicket})
        tradeData = response.json()

        response = rq.post(url + '/generateTradeTicketEmail', json={'tradeData':tradeData})
        message = response.json()['message']
        print('Email message:\n', message)

        response = rq.post(url + '/sendClientEmail', json={'message':message, 'clientEmail':'aa@agmtechnology.com', 'subject':'Test'})
        print('Email sent.', response.json())

    case '2':

        path = input('Enter the path: ')
        response = rq.post(url + '/getDocumentsFromCollection', json={'path':path})
        print(response.json())

    case '3':

        path = input('Enter the path: ')
        key = input('Enter the key: ')
        value = input('Enter the value: ')
        response = rq.post(url + '/queryDocumentsFromCollection', json={'path':path, 'key':key, 'value':value})
        print(response.json())
        
    case '4':

        response = rq.post(url + '/fetchReports', json={'queryIds':['732383', '734782', '742588']})
        print('Generating reports. Please wait.')
        flex_queries = response.json()

        response = rq.post(url + '/getSharedDriveInfo', json={'driveName':'ETL'})
        etl_id = response.json()['id']

        response = rq.post(url + '/getFolderInfo', json={'parentId':etl_id, 'folderName':'batch'})
        batch_folder_id = response.json()['id']

        response = rq.post(url + '/uploadCSVFiles', json={'files':flex_queries, 'parentId':batch_folder_id})
        print(response.json())

    case '':
        print('Exiting...')

    case _:
        print('Invalid choice')
