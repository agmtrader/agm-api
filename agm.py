from datetime import datetime

import pandas as pd

from helpers.FlexQuery import FlexQuery
from helpers.Google import Google

class AGM:
    
    def __init__(self):
        self.Email = Google().Gmail()
        self.Drive = Google().GoogleDrive()
        self.Database = Google().Firebase()
        
    # Returns a df of the flex query
    def fetchReports(self, queryIds):

        flexQuery = FlexQuery()

        # Get necessary data for request
        agmToken = "t=949768708375319238802665"

        print('Fetching Flex Query Service...')

        flex_queries = []

        for index, queryId in enumerate(queryIds):
            flex_query_df = flexQuery.getFlexQuery(agmToken, queryId)

            if not flex_query_df.empty:
                flex_query_dict = flex_query_df.to_dict(orient='records')[0]
                flex_query_dict['name'] = queryId
                flex_query_dict['index'] = index
                flex_queries.append(flex_query_dict)
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
