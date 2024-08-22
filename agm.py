from datetime import datetime

from helpers.FlexQuery import FlexQuery
from helpers.GoogleDrive import GoogleDrive

class AGM:
    
    def __init__(self):

        self.Drive = GoogleDrive()
        print(self.Drive.service)

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