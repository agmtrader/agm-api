import pandas as pd
from src.components.tools.reporting import get_open_positions_report, get_proposals_equity_report, get_rtd_report
from src.components.tools.risk_profiles import riskProfiles
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger
import numpy as np

@handle_exception
def create_investment_proposal(risk_profile_id: str):

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
    
    """
    ticker_list = proposals_equity_df['Ticker'].tolist()

    market_data_df = yf.download(ticker_list, period='max', interval='1d')
    print(market_data_df)
    """
    
    spy_df = proposals_equity_df[proposals_equity_df['sheet_name'] == 'SPY']
    spy_df = spy_df[['sheet_name', 'Date', 'Close']]

    # Compute SPY year-over-year yields for the most recent five 1-year periods
    try:
        spy_df_calc = spy_df.copy()
        spy_df_calc['Date'] = pd.to_datetime(spy_df_calc['Date'])
        spy_df_calc = spy_df_calc[['Date', 'Close']].dropna()
        spy_df_calc['Close'] = spy_df_calc['Close'].astype(float)
        spy_series = spy_df_calc.set_index('Date')['Close'].sort_index()

        if len(spy_series) >= 1250:  # roughly 5 years of trading days
            t_current = spy_series.index.max()
            t_minus_1y = t_current - pd.DateOffset(years=1)
            t_minus_2y = t_current - pd.DateOffset(years=2)
            t_minus_3y = t_current - pd.DateOffset(years=3)
            t_minus_4y = t_current - pd.DateOffset(years=4)
            t_minus_5y = t_current - pd.DateOffset(years=5)

            def price_asof(target_date: pd.Timestamp) -> float:
                return spy_series.loc[:target_date].iloc[-1]

            p_t = price_asof(t_current)
            p_t_1 = price_asof(t_minus_1y)
            p_t_2 = price_asof(t_minus_2y)
            p_t_3 = price_asof(t_minus_3y)
            p_t_4 = price_asof(t_minus_4y)
            p_t_5 = price_asof(t_minus_5y)

            y_t_to_t1 = (p_t / p_t_1) - 1.0
            y_t1_to_t2 = (p_t_1 / p_t_2) - 1.0
            y_t2_to_t3 = (p_t_2 / p_t_3) - 1.0
            y_t3_to_t4 = (p_t_3 / p_t_4) - 1.0
            y_t4_to_t5 = (p_t_4 / p_t_5) - 1.0

            yoy_yields = [y_t_to_t1, y_t1_to_t2, y_t2_to_t3, y_t3_to_t4, y_t4_to_t5]
            avg_yield = np.round(sum(yoy_yields) / len(yoy_yields), 4) * 100

            logger.announcement(
                f"SPY YoY yields — t→t-1y: {y_t_to_t1:.2%}, "
                f"t-1y→t-2y: {y_t1_to_t2:.2%}, "
                f"t-2y→t-3y: {y_t2_to_t3:.2%}, "
                f"t-3y→t-4y: {y_t3_to_t4:.2%}, "
                f"t-4y→t-5y: {y_t4_to_t5:.2%}. "
                f"Average: {avg_yield:.2%}"
            )
        else:
            logger.warning('Not enough SPY history to compute five 1-year period yields.')
    except Exception as exc:
        logger.error(f'Failed computing SPY YoY yields: {exc}')
    
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

    investment_proposal = [
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
    for asset_type in investment_proposal:
        percentage = risk_profile[asset_type['name']]
        assets_to_invest = int(total_assets * percentage)
        if asset_type['name'] == 'etfs':
            asset_type['bonds'].append({
                'Symbol_x': 'SPY',
                'Current Yield': avg_yield,
                'S&P Equivalent': 'ETF',
            })
        for equivalent in asset_type['equivalents']:
            # Filter bonds by S&P equivalent (ignoring + / - modifiers) and keep the highest yields
            equivalent_df = merged_df[
                merged_df['S&P Equivalent']
                    .astype(str)
                    .str.replace(r'[+\-]', '', regex=True)
                    == equivalent
            ]
            top_bonds = equivalent_df.head(assets_to_invest - len(asset_type['bonds']))
            asset_type['bonds'].extend(top_bonds[['Symbol_x', 'Current Yield', 'S&P Equivalent']].to_dict(orient='records'))

    # Print results
    for asset_type in investment_proposal:
        logger.announcement(f'Asset Type: {asset_type["name"]}')
        logger.announcement(f'Percentage: {risk_profile[asset_type["name"]]}')
        logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
        for bond in asset_type['bonds']:
            print(bond)
            logger.info(f'Bond: {bond["Symbol_x"]} - {bond["Current Yield"]} - {bond["S&P Equivalent"]}')

    # Create investment proposal
    #db.create(table='investment_proposal', data=investment_proposal)
    return investment_proposal