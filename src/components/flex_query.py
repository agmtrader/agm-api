import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv
from src.utils.logger import logger
from src.utils.exception import handle_exception

logger.announcement('Initializing Flex Query Service', type='info')
version='&v=3'
url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
logger.announcement('Initialized Flex Query Service', type='success')

@handle_exception
def getFlexQuery(token, queryId):

    logger.info(f'Getting Flex Query for queryId: {queryId}')

    xml_data = None

    retry_count = 0
    max_retries = 5
    retry_delay = 1

    try:
        
        # Create url for GET request to API for generating a report
        logger.info('Requesting Flex Query Template...')
        generatedTemplateURL = "".join([url, token, '&q=' + queryId, version])
        generatedTemplateResponse = rq.get(url=generatedTemplateURL)
        while generatedTemplateResponse.status_code != 200 and retry_count < max_retries:
            logger.warning(f'Flex Query Template Generation Failed. Preview: {generatedTemplateResponse.text[0:100]}')
            logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
            time.sleep(retry_delay)
            generatedTemplateResponse = rq.get(url=generatedTemplateURL)
            retry_count += 1
    
    except Exception as e:
        logger.error(f'Error requesting Flex Query Template: {str(e)}')
        raise Exception(f'Error requesting Flex Query Template: {str(e)}')

    logger.success('Flex Query Template Generated')

    # Populate ET element with generated report template
    tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
    root = tree.getroot()
    refCode = "&q=%s" % root.find('ReferenceCode').text

    try:

        # Create url for GET request to API to fetch generated report
        logger.info("Generating Flex Query...")
        generatedReportURL = root.find('Url').text
        generatedReportURL = "".join([generatedReportURL, "?",token, refCode, version])

        generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
        while (generatedReportResponse.status_code != 200 or ('ErrorCode' in generatedReportResponse.text and 'Fail' in generatedReportResponse.text)) and retry_count < max_retries:
            logger.warning(f'Flex Query Generation Failed. Preview: {generatedReportResponse.text[0:100]}')
            logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
            time.sleep(retry_delay)
            generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
            retry_count += 1

        if 'ErrorCode' in generatedReportResponse.text and 'Fail' in generatedReportResponse.text:
            logger.error(f'Flex Query Generation Failed. Error Code: {generatedReportResponse.text[0:100]}')
            raise Exception(f'Flex Query Generation Failed. Error Code: {generatedReportResponse.text[0:100]}')

    except Exception as e:
        logger.error(f'Error generating Flex Query: {str(e)}')
        raise Exception(f'Error generating Flex Query: {str(e)}')
    
    xml_data = generatedReportResponse.content
    df = binaryXMLtoDF(xml_data)
    logger.success(f"Flex Query generated")
    return df
    
# Returns dict of queryIds as keys and flex query as values
@handle_exception
def fetchFlexQueries(queryIds):
    agmToken = "t=949768708375319238802665"
    flex_queries = {}

    for _, queryId in enumerate(queryIds):

        flex_query_df = getFlexQuery(agmToken, queryId)
        flex_query_df['file_name'] = queryId

        try:
            flex_query_dict = flex_query_df.to_dict(orient='records')
            flex_queries[queryId] = flex_query_dict
        except Exception as e:
            logger.error(f'Flex Query Empty for queryId {queryId}: {str(e)}')
            raise Exception(f'Flex Query Empty for queryId {queryId}: {str(e)}')
        
    return flex_queries

@handle_exception
def binaryXMLtoDF(binaryXMLData):

    logger.info(f'Converting binary XML to DF')
    xml_data = binaryXMLData.decode('utf-8')
    reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

    rows = []

    for row in reader:
        if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row) and ('MSG' not in row):
            rows.append(row)
    
    df = pd.DataFrame(rows[1:], columns=rows[0])
    logger.success(f'Successfully converted binary XML to DF')
    return df