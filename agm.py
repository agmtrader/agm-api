import pandas as pd
import json
import csv
from datetime import datetime
import os

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from helpers.FlexQuery import FlexQuery

from helpers.GoogleDrive import GoogleDrive

class AGM:
    
    def __init__(self):

        self.Drive = GoogleDrive()

    def runETL(self):
        df = self.fetchReportsFromIBKR()
        print(df)

    def fetchReportsFromIBKR(self):

        flexQuery = FlexQuery()

        # Get necessary data for request
        queryIds = ['732383', '734782', '742588']
        agmToken = "t=949768708375319238802665"

        print('Fetching Flex Query Service...')

        for queryId in queryIds:
            flex_query_df = flexQuery.getFlexQuery(agmToken, queryId)

            if not flex_query_df.empty:
                print(flex_query_df)
            else:
                print('Flex Query Empty')

        # Get Batch folder ID
        etl_folder_info = self.Drive.getSharedDriveInfo('ETL')
        batch_folder_info = self.Drive.getFolderInfo(etl_folder_info['id'], 'batch')

        # Get files exported from Flex Web Service
        path_csv = 'csv/'
        csv_files = os.listdir(path_csv)

        # Upload each report
        for f in csv_files:

            # Upload batch file contents to server for new file
            media = MediaFileUpload(path_csv + f, mimetype='text/csv')

            # Create new file metadata with properties of original file and new destination
            file_metadata = {
                'name': f,
                'parents': [batch_folder_info['id']],
                'mimeType': 'text/csv'
            }

            # Create the new file in batch folder
            created_file = (
                self.Drive.service.files().create(
                supportsAllDrives=True,
                body=file_metadata,
                media_body=media,
                fields='id'
                )).execute()

            print('Stored file in batch:', f)

if __name__ == '__main__':
    AGM = AGM()