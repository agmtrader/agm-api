
import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv

from src.utils.logger import logger
from src.utils.connectors.drive import GoogleDrive
from datetime import datetime

logger.announcement('Initializing Flex Query Service', type='info')
Drive = GoogleDrive()
version='&v=3'
url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
logger.announcement('Initialized Flex Query Service', type='success')

def getFlexQuery(queryId):

    logger.info(f'Getting Flex Query for queryId: {queryId}')

    token = "t=419584539155539272816800"

    xml_data = None

    retry_count = 0
    max_retries = 10
    retry_delay = 2

    # Create url for GET request to API for generating a report
    logger.info('Requesting Flex Query Template...')
    generatedTemplateURL = "".join([url, token, '&q=' + queryId, version])
    generatedTemplateResponse = rq.get(url=generatedTemplateURL)

    # Retry while the response contains an ErrorCode (IBKR returns 200 even on errors)
    while ('ErrorCode' in generatedTemplateResponse.text) and retry_count < max_retries:
        logger.error(f'Flex Query Template Generation Failed. Preview: {generatedTemplateResponse.text[0:200]}')
        logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
        time.sleep(retry_delay)
        generatedTemplateResponse = rq.get(url=generatedTemplateURL)
        retry_count += 1

    # After retries, check again for ErrorCode and fail gracefully
    if 'ErrorCode' in generatedTemplateResponse.text:
        error_tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.text))
        error_root = error_tree.getroot()
        error_code = error_root.findtext('ErrorCode')
        error_message = error_root.findtext('ErrorMessage')
        logger.error(f'Flex Query Template Generation Failed. Error Code: {error_code}, Message: {error_message}')
        raise Exception(f'Flex Query Template Generation Failed. Error Code: {error_code}, Message: {error_message}')

    logger.success('Flex Query Template Generated')

    # Populate ET element with generated report template
    try:
        tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
        root = tree.getroot()
        refCode = "&q=%s" % root.find('ReferenceCode').text
    except Exception as e:
        logger.error(f'Error parsing Flex Query Template XML: {str(e)}')
        raise Exception(f'Error parsing Flex Query Template XML: {str(e)}')

    retry_count = 0
    # Create url for GET request to API to fetch generated report
    logger.info("Generating Flex Query...")
    generatedReportURL = root.find('Url').text
    generatedReportURL = "".join([generatedReportURL, "?",token, refCode, version])

    generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
    while 'ErrorCode' in generatedReportResponse.text and retry_count < max_retries:
        logger.error(f'Flex Query Generation Failed. Preview: {generatedReportResponse.text[0:200]}')
        logger.info(f'Retrying... Attempt {retry_count} of {max_retries}')
        time.sleep(retry_delay)
        generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
        retry_count += 1

    # After retries, check if still error
    if 'ErrorCode' in generatedReportResponse.text:
        # Try to extract error message
        error_tree = ET.ElementTree(ET.fromstring(generatedReportResponse.text))
        error_root = error_tree.getroot()
        error_code = error_root.findtext('ErrorCode')
        error_message = error_root.findtext('ErrorMessage')
        logger.error(f'Flex Query Generation Failed. Error Code: {error_code}, Message: {error_message}')
        raise Exception(f'Flex Query Generation Failed. Error Code: {error_code}, Message: {error_message}')

    # If response is HTML (e.g., error page), fail gracefully
    if generatedReportResponse.text.strip().startswith('<html'):
        logger.error('Flex Query Generation Failed. Received HTML error page from IBKR.')
        raise Exception('Flex Query Generation Failed. Received HTML error page from IBKR.')
    
    xml_data = generatedReportResponse.content
    df = binaryXMLtoDF(xml_data)
    logger.success(f"Flex Query generated")
    return df.to_dict(orient='records')

def binaryXMLtoDF(binaryXMLData):
    xml_data = binaryXMLData.decode('utf-8')

    reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

    rows = []
    header_row = None

    for row in reader:
        if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row) and ('MSG' not in row):
            # Capture the header row once and ignore subsequent duplicates
            if header_row is None:
                header_row = row
                rows.append(row)
            elif row == header_row:
                continue  # Skip duplicated header rows
            else:
                rows.append(row)
    
    df = pd.DataFrame(rows[1:], columns=rows[0])
    print(len(df))
    return df