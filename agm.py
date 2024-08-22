import pandas as pd
import json
import csv
from datetime import datetime
import os

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from helpers.FlexQuery import FlexQuery

from helpers.GoogleDrive import GoogleDrive

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import base64
from email.message import EmailMessage
import pandas as pd

from datetime import datetime
import pytz

class AGM:
    
    def __init__(self):

        self.Drive = GoogleDrive()

    def fetchReports(self, queryIds):

        flexQuery = FlexQuery()

        # Get necessary data for request
        agmToken = "t=949768708375319238802665"

        print('Fetching Flex Query Service...')

        flex_queries = []

        for queryId in queryIds:
            flex_query_df = flexQuery.getFlexQuery(agmToken, queryId)

            if not flex_query_df.empty:
                flex_queries.append(flex_query_df)
            else:
                print('Flex Query Empty')
        
        return flex_queries

if __name__ == '__main__':
    AGM = AGM()