
import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv

from src.utils.logger import logger
from src.utils.connectors.drive import GoogleDrive
from datetime import datetime

logger.announcement('Initializing Flex Query Service', type='info')
drive = GoogleDrive()
version='&v=3'
url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
logger.announcement('Initialized Flex Query Service', type='success')


def _extract_flex_error(response_text):
    if 'ErrorCode' not in response_text:
        return None, None
    try:
        error_root = ET.fromstring(response_text)
        return error_root.findtext('ErrorCode'), error_root.findtext('ErrorMessage')
    except ET.ParseError:
        return 'UNKNOWN', 'Unable to parse IBKR error payload.'

def _poll_flex_response(request_fn, operation_name, max_retries=20, retry_delay_seconds=5):
    attempt = 0

    while attempt < max_retries:
        response = request_fn()
        error_code, error_message = _extract_flex_error(response.text)

        if error_code is None:
            return response

        if error_code != '1019':
            logger.error(
                f'{operation_name} Failed. Error Code: {error_code}, Message: {error_message}'
            )
            raise Exception(
                f'{operation_name} Failed. Error Code: {error_code}, Message: {error_message}'
            )

        if attempt >= max_retries - 1:
            logger.error(
                f'{operation_name} Failed after {max_retries} attempts. '
                f'Error Code: {error_code}, Message: {error_message}'
            )
            raise Exception(
                f'{operation_name} Failed after {max_retries} attempts. '
                f'Error Code: {error_code}, Message: {error_message}'
            )

        logger.error(f'{operation_name} Failed. Preview: {response.text[0:200]}')
        logger.info(
            f'{operation_name} in progress (code 1019). Retry attempt {attempt + 1} '
            f'after {retry_delay_seconds}s.'
        )
        time.sleep(retry_delay_seconds)
        attempt += 1

    raise Exception(f'{operation_name} Failed after {max_retries} attempts.')

def getFlexQuery(queryId):

    logger.info(f'Getting Flex Query for queryId: {queryId}')

    token = "t=707418312601144786944"

    xml_data = None

    # Create url for GET request to API for generating a report
    logger.info('Requesting Flex Query Template...')
    generatedTemplateURL = "".join([url, token, '&q=' + queryId, version])
    generatedTemplateResponse = _poll_flex_response(
        request_fn=lambda: rq.get(url=generatedTemplateURL),
        operation_name='Flex Query Template Generation',
    )

    logger.success('Flex Query Template Generated')

    # Populate ET element with generated report template
    try:
        tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
        root = tree.getroot()
        refCode = "&q=%s" % root.find('ReferenceCode').text
    except Exception as e:
        logger.error(f'Error parsing Flex Query Template XML: {str(e)}')
        raise Exception(f'Error parsing Flex Query Template XML: {str(e)}')

    # Create url for GET request to API to fetch generated report
    logger.info("Generating Flex Query...")
    generatedReportURL = root.find('Url').text
    generatedReportURL = "".join([generatedReportURL, "?",token, refCode, version])
    generatedReportResponse = _poll_flex_response(
        request_fn=lambda: rq.get(url=generatedReportURL, allow_redirects=True),
        operation_name='Flex Query Generation',
    )

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

    rows_count = 0

    for row in reader:
        rows_count += 1
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
    return df
