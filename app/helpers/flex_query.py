import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv

from app.helpers.logger import logger
from app.helpers.response import Response

from datetime import datetime

logger.info('Initializing Flex Query Service')
version='&v=3'
url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
logger.info('Initialized Flex Query Service')

def getFlexQuery(token, queryId):
    try:
        # Create url for GET request to API for generating a report
        logger.info('Requesting Flex Query Template...')
        generatedTemplateURL = "".join([url, token, '&q=' + queryId, version])
        generatedTemplateResponse = rq.get(url=generatedTemplateURL)
        while generatedTemplateResponse.status_code != 200:
            time.sleep(1)
            logger.info('Retrying...', generatedTemplateResponse.content)
            generatedTemplateResponse = rq.get(url=generatedTemplateURL)
        logger.info('Flex Query Template Generated')

        # Populate ET element with generated report template
        tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
        root = tree.getroot()
        refCode = "&q=%s" % root.find('ReferenceCode').text

        # Create url for GET request to API to fetch generated report
        logger.info("Generating Flex Query...")
        generatedReportURL = root.find('Url').text
        generatedReportURL = "".join([generatedReportURL, "?",token, refCode, version])
        generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
        while generatedReportResponse.status_code != 200:
            time.sleep(1)
            logger.info('Retrying...', generatedReportResponse.content)
            generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
        xml_data = generatedReportResponse.content
        logger.info("Flex Query generated.")

        # Create a CSV file backup of the Flex Query
        now = datetime.now()
        now = now.strftime('%Y%m%d%H%M%S')

        df = binaryXMLtoDF(xml_data)
        logger.info(df)

        return Response.success(df)
    except Exception as e:
        logger.error(f"Error in getFlexQuery: {str(e)}")
        return Response.error(f"Failed to get Flex Query: {str(e)}")

def binaryXMLtoCSV(binaryXMLData, file_name):
    try:
        xml_data = binaryXMLData.decode('ascii')
        reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

        with open('backups/acobo/' + file_name + '.csv',  'w') as out_file:
            writer = csv.writer(out_file)
            for row in reader:
                if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row):
                    writer.writerow(row)

        return Response.success('backups/acobo/' + file_name + '.csv')
    except Exception as e:
        logger.error(f"Error in binaryXMLtoCSV: {str(e)}")
        return Response.error(f"Failed to convert binary XML to CSV: {str(e)}")

def binaryXMLtoDF(binaryXMLData):
    xml_data = binaryXMLData.decode('ascii')
    reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

    rows = []

    for row in reader:
      if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row):
        rows.append(row)
    
    df = pd.DataFrame(rows[1:], columns=rows[0])
    return df