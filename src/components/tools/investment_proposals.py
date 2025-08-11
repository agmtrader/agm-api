import pandas as pd
from src.components.tools.reporting import get_open_positions_report, get_proposals_equity_report, get_rtd_report
from src.components.tools.risk_profiles import riskProfiles
from yfinance import *
from src.utils.logger import logger
from datetime import datetime, timedelta
import yfinance as yf

def fetch_historical_prices_etfs(etfs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch historical prices for ETFs using yfinance
    """
    logger.announcement('Fetching historical prices for ETFs...')
    return
    # Create a list of tickers to fetch

def generate_investment_proposal(risk_profile_id: str):

    logger.announcement('Generating investment proposal...')

    # Get open positions
    open_positions = get_open_positions_report()
    open_positions_df = pd.DataFrame(open_positions)

    # Extract all unique bonds
    bonds_df = open_positions_df[open_positions_df['AssetClass'] == 'BOND']
    bonds_df_no_duplicates = bonds_df.drop_duplicates(subset=['Symbol'])
    logger.announcement(f'Total bonds: {len(bonds_df)}')
    logger.announcement(f'Total unique bonds: {len(bonds_df_no_duplicates)}')

    # Get proposals equity report
    proposals_equity = get_proposals_equity_report()
    proposals_equity_df = pd.DataFrame(proposals_equity)

    # Extract only ETFs from State Street Family
    etfs_df = proposals_equity_df[proposals_equity_df['Family'] == 'State Street']
    print(etfs_df)
    
    # Fetch historical prices for ETFs
    etfs_historical_df = fetch_historical_prices_etfs(etfs_df)
    print(etfs_historical_df)

    # Get RTD report
    rtd_report = get_rtd_report()
    rtd_df = pd.DataFrame(rtd_report)

    # Remove IBCID Symbol column
    rtd_df['Symbol'] = (
        rtd_df['Symbol']
            .astype(str)
            .str.strip()
            .str.replace(r'^IBCID', '', regex=True)
    )
    rtd_df['Symbol'] = pd.to_numeric(rtd_df['Symbol'], errors='coerce').astype('Int64')

    # Merge bonds with RTD
    merged_df = pd.merge(bonds_df_no_duplicates, rtd_df, left_on='Conid', right_on='Symbol', how='left')
    logger.announcement(f'Total bonds with RTD: {len(merged_df)}')

    # Post processing
    merged_df = merged_df[merged_df['Current Yield'].notna() & (merged_df['Current Yield'] != '')]
    merged_df['Current Yield'] = (
        merged_df['Current Yield']
            .astype(str)
            .str.replace('%', '', regex=False)
            .astype(float)
            .round(2)
    )
    merged_df = (
        merged_df
            .sort_values(by='Current Yield', ascending=False)
            .reset_index(drop=True)
    )

    # Fill missing values with 0 for numeric columns and empty string for non-numeric columns
    numeric_cols = merged_df.select_dtypes(include=['number']).columns
    merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0)

    # Fill missing values with empty string for non-numeric columns
    non_numeric_cols = merged_df.select_dtypes(exclude=['number']).columns
    merged_df[non_numeric_cols] = merged_df[non_numeric_cols].fillna('')

    # Get risk profile
    risk_profile = next((rp for rp in riskProfiles if str(rp['id']) == str(risk_profile_id)), None)
    if not risk_profile:
        raise Exception(f'Risk profile with id {risk_profile_id} not found')

    bond_asset_types = [
        {
            'name': 'bonds_aaa_a',
            'equivalents': ['AAA', 'AA', 'A'],
            'bonds': []
        },
        {
            'name': 'bonds_bbb',
            'equivalents': ['BBB'],
            'bonds': []
        },
        {
            'name': 'bonds_bb',
            'equivalents': ['BB'],
            'bonds': []
        },
        {
            'name': 'etfs',
            'equivalents': ['ETF'],
            'bonds': []
        }
    ]

    total_assets = 20

    # Populate bonds for each asset type
    for asset_type in bond_asset_types:
        percentage = risk_profile[asset_type['name']]
        assets_to_invest = int(total_assets * percentage)
        if asset_type['name'] == 'etfs':
            continue
        for equivalent in asset_type['equivalents']:
            # Filter bonds by S&P equivalent (ignoring + / - modifiers) and keep the highest yields
            equivalent_df = merged_df[
                merged_df['S&P Equivalent']
                    .astype(str)
                    .str.replace(r'[+\-]', '', regex=True)
                    == equivalent
            ]
            top_bonds = equivalent_df.head(assets_to_invest - len(asset_type['bonds']))
            asset_type['bonds'].extend(top_bonds.to_dict(orient='records'))

    # Print results
    for asset_type in bond_asset_types:
        logger.announcement(f'Asset Type: {asset_type["name"]}')
        logger.announcement(f'Percentage: {risk_profile[asset_type["name"]]}')
        logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
        for bond in asset_type['bonds']:
            logger.info(f'Bond: {bond["Symbol_x"]} - {bond["Current Yield"]} - {bond["S&P Equivalent"]}')

    return bond_asset_types