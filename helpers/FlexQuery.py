import requests as rq
import xml.etree.ElementTree as ET
import time
import pandas as pd
import csv

from datetime import datetime

class FlexQuery:

  def __init__(self):
    self.version='&v=3'
    self.url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?"
    print('Initialized FlexQuery Service')

  def getFlexQuery(self, token, queryId):

    # Crete url for GET request to API for generating a report
    generatedTemplateURL = "".join([self.url, token, '&q=' + queryId, self.version])

    # Make a GET request to the API
    generatedTemplateResponse = rq.get(url=generatedTemplateURL)

    while generatedTemplateResponse.status_code != 200:
      print('Retrying...', generatedTemplateResponse.content)

    # Populate ET element with generated report template!!
    tree = ET.ElementTree(ET.fromstring(generatedTemplateResponse.content))
    root = tree.getroot()

    # Find reference code of generated report
    refCode = "&q=%s" % root.find('ReferenceCode').text

    # Create url for GET request to API to fecth generated report
    generatedReportURL = root.find('Url').text
    generatedReportURL = "".join([generatedReportURL, "?",token, refCode, self.version])

    # Wait for generation to finish
    print("Generating Flex Query...")
    generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)
    
    while generatedReportResponse.status_code != 200:
      time.sleep(1)
      print('Retrying...', generatedReportResponse.content)
      generatedReportResponse = rq.get(url=generatedReportURL, allow_redirects=True)

    xml_data = generatedReportResponse.content

    print("Flex Query generated.")

    # Create a CSV file backup of the Flex Query
    now = datetime.now()
    now = now.strftime('%Y%m%d%H%M%S')

    df = self.binaryXMLtoDF(xml_data)
    print(df)

    return df

  def binaryXMLtoCSV(self, binaryXMLData, file_name):

      xml_data = binaryXMLData.decode('ascii')
      reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

      with open('backups/acobo/' + file_name + '.csv',  'w') as out_file:
          writer = csv.writer(out_file)
          for row in reader:
            if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row):
              writer.writerow(row)

      return 'backups/acobo/' + file_name + '.csv'

  def binaryXMLtoDF(self, binaryXMLData):
      xml_data = binaryXMLData.decode('ascii')
      reader = csv.reader(xml_data.splitlines(), skipinitialspace=True)

      rows = []

      for row in reader:
        if ('BOA' not in row) and ('BOF' not in row) and ('BOS' not in row) and ('EOS' not in row) and ('EOA' not in row) and ('EOF' not in row):
          rows.append(row)
      
      df = pd.DataFrame(rows[1:], columns=rows[0])
      return df

if __name__ == '__main__':
  print('')