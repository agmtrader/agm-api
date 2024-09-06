import pandas as pd

from helpers.FlexQuery import FlexQuery
from helpers.Google import Google

from pandas.tseries.offsets import BDay
from datetime import datetime
import pytz

import requests as rq

class AGM:
    
    def __init__(self):
        self.Email = Google().Gmail()
        self.Drive = Google().GoogleDrive()
        self.Database = Google().Firebase()
        self.FlexQuery = FlexQuery()
        
    # Returns a df of the flex query
    def fetchFlexQueries(self, queryIds):

        # Get necessary data for request
        agmToken = "t=949768708375319238802665"

        flex_queries = {}

        for index, queryId in enumerate(queryIds):
            flex_query_df = self.FlexQuery.getFlexQuery(agmToken, queryId)
            flex_query_df['file_name'] = queryId

            if not flex_query_df.empty:
                flex_query_dict = flex_query_df.to_dict(orient='records')
                flex_queries[queryId] = flex_query_dict
            else:
                print(f'Flex Query Empty for index {index}')
        
        return flex_queries

    # Returns a dictionary of trade data
    def processTradeTicket(self, flex_query, indices):
        
        flex_query_df = pd.DataFrame(flex_query)

        # Create dataframe with indexed rows only
        df_indexed = flex_query_df.iloc[indices].copy()

        # Get relevant conid

        # Process each trade first
        # Add Coupon, and Maturity

        df_indexed['Coupon'] = 0.0  # Placeholder
        df_indexed['Maturity'] = ''  # Placeholder

        # Absolute value
        df_indexed.loc[:,'Quantity'] = df_indexed['Quantity'].astype(float).abs()
        df_indexed.loc[:,'AccruedInterest'] = df_indexed['AccruedInterest'].astype(float).abs()

        df_indexed.loc[:,'NetCash'] = df_indexed['NetCash'].astype(float).abs()
        df_indexed.loc[:,'Amount'] = df_indexed['NetCash'].astype(float).abs()

        # Apply formulas to each trade
        try:
            df_indexed.loc[:,'Accrued (Days)'] = round((df_indexed['AccruedInterest'].astype(float)) / (df_indexed['Coupon'].astype(float)/100 * df_indexed['Quantity'].astype(float)) * 360).astype(float)
        except:
            df_indexed.loc[:,'Accrued (Days)'] = 0

        df_indexed.loc[:,'TotalAmount'] = round(df_indexed['AccruedInterest'] + df_indexed['NetCash'], 2).astype(float)
        df_indexed.loc[:,'Price (including Commissions)'] = round((df_indexed['NetCash']/df_indexed['Quantity']) * 100, 4).astype(float)

        df_indexed['Price'] = df_indexed['Price'].astype(float)

        # Process a single consolidated trade confirmation
        df_consolidated = df_indexed.iloc[0:1].copy()

        if (len(df_indexed) > 1):

            # Replace info with new info
            df_consolidated.loc[:, 'Quantity'] = df_indexed['Quantity'].sum()

            df_consolidated.loc[:, 'AccruedInterest'] = df_indexed['AccruedInterest'].sum()

            df_consolidated.loc[:, 'NetCash'] = df_indexed['NetCash'].sum()
            df_consolidated.loc[:, 'Amount'] = df_indexed['NetCash'].sum()

            df_consolidated.loc[:, 'Price'] = df_indexed['Price'].sum()/len(df_indexed).astype(float)

            df_consolidated.loc[:, 'Exchange'] = ''

            df_consolidated.loc[:,'Accrued (Days)'] = round((df_consolidated['AccruedInterest'].astype(float)) / (df_consolidated['Coupon'].astype(float)/100 * df_consolidated['Quantity'].astype(float)) * 360).astype(float)
            df_consolidated.loc[:,'TotalAmount'] = round(df_consolidated['AccruedInterest'] + df_consolidated['NetCash'], 2).astype(float)
            df_consolidated.loc[:,'Price (including Commissions)'] = round((df_consolidated['NetCash']/df_consolidated['Quantity']) * 100, 4).astype(float)

        # Generate email message
        trade_confirmation_columns = [
        "ClientAccountID",
        "AccountAlias",
        "CurrencyPrimary",

        "AssetClass",
        "Symbol",
        "Description",
        "Conid",
        "SecurityID",
        "SecurityIDType",
        "CUSIP",
        "ISIN",
        "FIGI",
        "Issuer",
        "Maturity",

        "Buy/Sell",
        "SettleDate",
        "TradeDate",
        "Exchange",
        "Quantity",
        "AccruedInterest",
        "Accrued (Days)",
        "Price",
        "Price (including Commissions)",
        "Amount",
        "SettleDate",
        "TradeDate",
        "TotalAmount"
        ]

        # Fill dictionary with trade data
        tradeData = {}

        df_consolidated.fillna('', inplace=True)

        for key in trade_confirmation_columns:
            tradeData[key] = df_consolidated.iloc[0][key]

        return tradeData

    # Returns a string of the email message
    def generateTradeTicketEmail(self, tradeData):

        # Create message from dictionary
        message = ''
        skips = ['FIGI', 'CurrencyPrimary',' Maturity']

        for key, value in tradeData.items():
            message += str(str(key) + ': ' + str(value) + '\n')
            if (key in skips):
                message += '\n'

        return {'message':message}

class Reporting:

    def __init__(self):
        self.Drive = Google().GoogleDrive()
        self.url = 'http://127.0.0.1:5000'
        # Get relevant folder IDs

        self.etl_id = self.Drive.getSharedDriveInfo('ETL')['id']
        self.batch_folder_id = self.Drive.getFolderInfo(self.etl_id, 'batch')['id']

        # Get the current time in CST
        cst = pytz.timezone('America/Costa_Rica')
        cst_time = datetime.now(cst)
        today_date = cst_time.strftime('%Y%m%d%H%M')
        self.yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
        self.first_date = cst_time.replace(day=1).strftime('%Y%m%d')
        current_year = cst_time.year

        self.batch_files = []

    def ETL(self):
        self.fetchReports()
        batch_files = self.renameFilesInBatch()
        self.sortFilesToFolders(batch_files)

    def fetchReports(self):
        # Fetch reports from Flex Query Service
        #response = rq.post(url + '/fetchReports', json={'queryIds':['732383', '734782', '742588']})
        response = rq.post(self.url + '/fetchFlexQueries', json={'queryIds':['732383']})
        print('\nGenerating reports. Please wait.\n')
        flex_queries = response.json()

        # Get relevant folder IDs
        etl_id = self.Drive.getSharedDriveInfo('ETL')['id']
        batch_folder_id = self.Drive.getFolderInfo(etl_id, 'batch')['id']

        # Upload files to batch folder
        self.Drive.uploadCSVFiles(flex_queries, batch_folder_id)
        return batch_folder_id

    def renameFilesInBatch(self):
        
        # Get all files in batch
        batch_files = self.Drive.getFilesInFolder(self.batch_folder_id)
        print('Batch files: ', batch_files, '\n')
        input('Press Enter to continue. Check batch files first.')

        # Rename files
        # Add new name to each file
        for f in batch_files:
            match f['name']:
                case '742588':
                    f['new_name'] = ('742588_' + self.yesterday_date + '.csv')
                case '734782':
                    f['new_name'] = ('734782_' + self.yesterday_date + '.csv')
                case '732383':
                    f['new_name'] = ('732383_' + self.first_date + '_' + self.yesterday_date + '.csv')
                case 'clients':
                    f['new_name'] = ('clients ' + self.today_date + ' agmtech212' + '.xls')
                case 'tasks_for_subaccounts':
                    f['new_name'] = ('tasks_for_subaccounts ' + self.today_date + ' agmtech212' + '.csv')
                case 'ContactListSummary':
                    f['new_name'] = ('ContactListSummary ' + self.today_date + ' agmtech212' + '.csv')
                case _:
                    f['new_name'] = f['name']

        batch_files = self.Drive.renameFiles(batch_files)
        return batch_files

    def sortFilesToFolders(self, batch_files):
        # Sort files into folders
        # Get Interactive Brokers Shared Drive ID
        ibkr_folder_info = self.Drive.getSharedDriveInfo('Interactive Brokers')
        # Get Queries folder ID
        queries_folder_info = self.Drive.getFolderInfo(ibkr_folder_info['id'], 'Queries')
        """"""
        # Get Parent Tasks For Sub Accounts folder ID
        parent_subaccounts_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'Tasks For Sub Accounts')
        # Get this year's Tasks For Sub Accounts folder ID
        subaccounts_folder_info = self.Drive.getFolderInfo(parent_subaccounts_folder_info['id'], f'tasks_for_sub_accounts_{self.current_year}')
        """"""
        # Get Parent Contact List Summary folder ID
        parent_contacts_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'Contact List Summary')
        # Get this year's Contact List Summary folder ID
        contacts_folder_info = self.Drive.getFolderInfo(parent_contacts_folder_info['id'], f'Contact List Summary {self.current_year}')
        """"""
        # Get Parent RTD folder ID
        parent_rtd_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'RTD')
        # Get this year's RTD folder ID
        rtd_folder_info = self.Drive.getFolderInfo(parent_rtd_folder_info['id'], f'RTD_{self.current_year}')
        """"""
        # Get Clients folder ID
        clients_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'Clients')
        """"""
        # Get Open Positions folder ID
        open_positions_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'Open Positions')
        # Get parent 742588 folder ID
        parent_742588_folder_info = self.Drive.getFolderInfo(open_positions_folder_info['id'], '742588')
        # Get this year's 742588 folder ID
        folder_742588_info = self.Drive.getFolderInfo(parent_742588_folder_info['id'], f'742588_{self.current_year}')
        """"""
        # Get NAV in Base folder ID
        nav_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'NAV in Base')
        # Get parent 734782 folder ID
        parent_734782_folder_info = self.Drive.getFolderInfo(nav_folder_info['id'], '734782')
        # Get this year's 734782 folder ID
        folder_734782_info = self.Drive.getFolderInfo(parent_734782_folder_info['id'], f'734782_{self.current_year}')
        """"""
        # Get Client Fees folder ID
        client_fees_folder_info = self.Drive.getFolderInfo(queries_folder_info['id'], 'Client Fees')
        # Get parent 734782 folder ID
        parent_732383_folder_info = self.Drive.getFolderInfo(client_fees_folder_info['id'], '732383')
        # Get this year's 734782 folder ID
        folder_732383_info = self.Drive.getFolderInfo(parent_732383_folder_info['id'], f'732383_{self.current_year}')
        # Print all folder information
        if False:
            print('IBKR folder info:', ibkr_folder_info)

            print('Queries folder info:', queries_folder_info)

            print('Parent subaccounts folder info:', parent_subaccounts_folder_info)
            print('Subaccounts folder info:', subaccounts_folder_info)

            print('Parent contacts folder info:', parent_contacts_folder_info)
            print('Contacts folder info:', contacts_folder_info)

            print('Parent RTD folder info:', parent_rtd_folder_info)
            print('RTD folder info:', rtd_folder_info)

            print('Clients folder info:', clients_folder_info)

            print('Open positions folder info:', open_positions_folder_info)
            print('Parent 742588 folder info:', parent_742588_folder_info)
            print('742588 folder info:', folder_742588_info)

            print('NAV folder info:', nav_folder_info)
            print('Parent 734782 folder info:', parent_734782_folder_info)
            print('734782 folder info:', folder_734782_info)

            print('Client fees folder info:', client_fees_folder_info)
            print('Parent 732383 folder info:', parent_732383_folder_info)
            print('732383 folder info:', folder_732383_info)
        
        # Move files to respective folder in backups
        for f in batch_files:
            
            # Set new file's destination
            match f['name']:
                case name if 'clients' in name:
                    new_parent_id = clients_folder_info['id']
                case name if 'ContactListSummary' in name:
                    new_parent_id = contacts_folder_info['id']
                case name if 'tasks_for_subaccounts' in name:
                    new_parent_id = subaccounts_folder_info['id']
                case name if 'RTD' in name:
                    new_parent_id = rtd_folder_info['id']
                case name if '742588' in name:
                    new_parent_id = folder_742588_info['id']
                case name if '734782' in name:
                    new_parent_id = folder_734782_info['id']
                case name if '732383' in name:
                    new_parent_id = folder_732383_info['id']
                case _:
                    new_parent_id = self.etl_id

            updated_file = self.Drive.moveFile(f, new_parent_id)
            print(f"File '{f['name']}' moved to new parent folder. {updated_file}")
