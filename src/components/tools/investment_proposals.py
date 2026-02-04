import pandas as pd
from src.components.tools.reporting import get_open_positions_report, get_proposals_equity_report, get_rtd_report
from src.components.tools.risk_profiles import risk_archetypes
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger
import numpy as np

@handle_exception
def create_investment_proposal(risk_profile: dict = None, assets: list[dict] = None):

    logger.announcement('Generating investment proposal...')

    try:

        # Get open positions
        open_positions = get_open_positions_report()
        open_positions_df = pd.DataFrame(open_positions)

        # Extract all unique bonds
        bonds_df = open_positions_df[open_positions_df['AssetClass'] == 'BOND']
        bonds_df_no_duplicates = bonds_df.drop_duplicates(subset=['Symbol_y'])
        logger.announcement(f'Total bonds: {len(bonds_df)}')
        logger.announcement(f'Total unique bonds: {len(bonds_df_no_duplicates)}')

        # Get proposals equity report
        proposals_equity = get_proposals_equity_report()
        proposals_equity_df = pd.DataFrame(proposals_equity)
        
        """
        ticker_list = proposals_equity_df['Ticker'].tolist()

        market_data_df = yf.download(ticker_list, period='max', interval='1d')
        """
        
        spy_df = proposals_equity_df[proposals_equity_df['sheet_name'] == 'SPY']
        spy_df = spy_df[['sheet_name', 'Date', 'Close']]

        # Compute SPY year-over-year yields for the most recent five 1-year periods
        avg_yield = None

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
        else:
            logger.warning('Not enough SPY history to compute five 1-year period yields.')
        
        # Get RTD report
        rtd_report = get_rtd_report()
        rtd_df = pd.DataFrame(rtd_report)

        # Remove IBCID Symbol column if it exists and clean it (though not strictly used for merge anymore)
        if 'Symbol' in rtd_df.columns:
             # logger.announcement(rtd_df['Symbol'].head(10))
             pass

        # Use RTD report as the base for candidates
        merged_df = rtd_df.copy()
        
        # Rename columns to match expected format (using _x suffix as legacy from previous merge)
        merged_df = merged_df.rename(columns={
            'Financial Instrument': 'Symbol_x',
            'Current Yield': 'Current Yield_x',
            'S&P Equivalent': 'S&P Equivalent_x',
            'Issuer': 'Ticker' # Or derived below
        })
        
        logger.announcement(f'Total bonds from RTD: {len(merged_df)}')
        logger.info(f"Merged DF columns: {merged_df.columns.tolist()}")

        if 'S&P Equivalent_x' in merged_df.columns:
             logger.info(f"S&P Equivalent_x unique values: {merged_df['S&P Equivalent_x'].unique()}")

        # Post processing
        merged_df = merged_df[merged_df['Current Yield_x'] != '']
        
        # Extract ticker (issuer) if not already present or correct
        # Depending on RTD content, Issuer column might be sufficient or we extract from Financial Instrument
        if 'Ticker' not in merged_df.columns or merged_df['Ticker'].isnull().all():
             merged_df['Ticker'] = (
                merged_df['Symbol_x']
                    .astype(str)
                    .str.strip()
                    .str.split()  # split by whitespace
                    .str[0]
            )

        merged_df['Current Yield_x'] = (
            merged_df['Current Yield_x']
                .astype(str)
                .str.replace('%', '', regex=False)
                .astype(float)
                .round(2)
        )

        merged_df = (
            merged_df
                .sort_values(by='Current Yield_x', ascending=False)
                .reset_index(drop=True)
        )

        # Fill missing values with 0 for numeric columns and empty string for non-numeric columns
        numeric_cols = merged_df.select_dtypes(include=['number']).columns
        merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0)

        # Fill missing values with empty string for non-numeric columns
        non_numeric_cols = merged_df.select_dtypes(exclude=['number']).columns
        merged_df[non_numeric_cols] = merged_df[non_numeric_cols].fillna('')

        risk_profile_id = risk_profile.get('id') if isinstance(risk_profile, dict) else None

        investment_proposal = [
            {
                'name': 'bonds_aaa_a',
                'equivalents': ['AAA', 'AAA-', 'AAA+', 'AA', 'AA+', 'AA-', 'A', 'A-', 'A+'],
                'bonds': []
            },
            {
                'name': 'bonds_bbb',
                'equivalents': ['BBB', 'BBB-', 'BBB+'],
                'bonds': []
            },
            {
                'name': 'bonds_bb',
                'equivalents': ['BB', 'BB-', 'BB+'],
                'bonds': []
            },
            {
                'name': 'etfs',
                'equivalents': ['ETF'],
                'bonds': []
            }
        ]

        total_assets = 20

        def get_bucket_for_rating(rating: str) -> str:
            normalized = str(rating).strip().upper().replace('+', '').replace('-', '')
            if normalized == 'ETF':
                return 'etfs'
            if normalized in {'AAA', 'AA', 'A'}:
                return 'bonds_aaa_a'
            if normalized == 'BBB':
                return 'bonds_bbb'
            if normalized == 'BB':
                return 'bonds_bb'
            return ''

        use_assets = assets is not None
        if use_assets:
            if not isinstance(assets, list):
                raise Exception('assets must be a list of dicts with symbol and percentage.')

            logger.announcement(f'Creating proposal from {len(assets)} assets.')

            for asset in assets:
                if not isinstance(asset, dict):
                    raise Exception('Each asset must be a dict with symbol and percentage.')

                symbol = asset.get('symbol')
                percentage = asset.get('percentage')

                if not symbol or percentage is None:
                    raise Exception('Each asset must include symbol and percentage.')

                rtd_match = rtd_df[rtd_df['Symbol'] == symbol]
                if rtd_match.empty:
                    raise Exception(f'IBCID "{symbol}" not found in RTD report.')

                rtd_row = rtd_match.iloc[0].to_dict()
                rating = rtd_row.get('S&P Equivalent') or rtd_row.get('SP')
                if not rating:
                    raise Exception(f'No rating found in RTD report for IBCID "{symbol}".')

                bucket_name = get_bucket_for_rating(rating)
                if not bucket_name:
                    raise Exception(f'Unknown rating "{rating}" for asset {symbol}.')

                bucket = next(b for b in investment_proposal if b['name'] == bucket_name)
                bucket['bonds'].append({
                    'Symbol_x': str(rtd_row.get('Financial Instrument') or symbol),
                    'Current Yield_x': float(rtd_row.get('Current Yield') or 0),
                    'S&P Equivalent_x': str(rating),
                    'percentage': float(percentage),
                    'ibcid': str(symbol),
                })

            for asset_type in investment_proposal:
                logger.announcement(f'Asset Type: {asset_type["name"]}')
                logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
                for bond in asset_type['bonds']:
                    logger.info(
                        f'Bond: {bond["Symbol_x"]} - {bond.get("percentage", 0)} - {bond["S&P Equivalent_x"]}'
                    )
        if not use_assets:
            if not risk_profile:
                raise Exception('Risk profile is required when assets are not provided.')

            logger.announcement(f'Risk profile: {risk_profile}')
            risk_score = risk_profile['score']
            risk_profile_id = risk_profile['id']
            risk_archetype = next(
                (rp for rp in risk_archetypes
                 if float(rp['min_score']) <= float(risk_score) and float(rp['max_score']) >= float(risk_score)),
                None
            )
            if not risk_archetype:
                logger.error(f'Risk profile with score {risk_score} not found')
                raise Exception(f'Risk profile with score {risk_score} not found')

            # Populate bonds for each asset type
            # Keep track of already selected tickers to avoid duplicates across buckets
            used_symbols: set[str] = set()
            
            # Initialize used_symbols with tickers from open positions to avoid recommending what is already owned
            if not bonds_df_no_duplicates.empty:
                 # Assuming 'Issuer_x' or 'Symbol_x' contains the ticker in open_positions
                 # Based on previous code, Ticker was derived from Symbol_x
                 if 'Symbol_x' in bonds_df_no_duplicates.columns:
                     existing_tickers = bonds_df_no_duplicates['Symbol_x'].astype(str).str.strip().str.split().str[0].tolist()
                     used_symbols.update(existing_tickers)
                     logger.info(f"Initialized used_symbols with {len(used_symbols)} existing tickers.")

            for asset_type in investment_proposal:
                percentage = risk_archetype[asset_type['name']]
                assets_to_invest = int(total_assets * percentage)

                logger.info(f"--- Processing bucket: {asset_type['name']} ---")
                logger.info(f"Assets to invest: {assets_to_invest}")
                logger.info(f"Current bonds in bucket: {len(asset_type['bonds'])}")

                # Special handling for ETF bucket – always add SPY first
                if asset_type['name'] == 'etfs':
                    asset_type['bonds'].append({
                        'Symbol_x': 'SPY',
                        'Current Yield_x': float(avg_yield) if avg_yield is not None else 0.0,
                        'S&P Equivalent_x': 'ETF',
                    })
                    used_symbols.add('SPY')

                # Build a combined dataframe for all equivalents that belong to this asset type
                sanitized_equivalents = asset_type['equivalents']
                
                # Log what we are looking for
                logger.info(f"Sanitized equivalents for {asset_type['name']}: {sanitized_equivalents}")
                
                combined_df = merged_df[
                    merged_df['S&P Equivalent_x']
                        .astype(str)
                        .str.replace(r'[+\-]', '', regex=True)
                        .isin(sanitized_equivalents)
                ]
                
                logger.info(f"Found {len(combined_df)} matches for {asset_type['name']}")
                if len(combined_df) == 0:
                     # Debug why no matches if we expected some
                     logger.info("Debugging first 5 sanitized S&P values from merged_df:")
                     debug_vals = merged_df['S&P Equivalent_x'].astype(str).str.replace(r'[+\-]', '', regex=True).head()
                     logger.info(debug_vals)

                # Deduplicate by ticker and order by yield
                combined_df = (
                    combined_df
                        .sort_values(by='Current Yield_x', ascending=False)
                        .groupby('Ticker')  # one bond per issuer ticker across coupons/maturities
                        .head(1)
                )

                # Exclude tickers that have already been used in previous buckets
                combined_df = combined_df[~combined_df['Ticker'].isin(used_symbols)]

                # Take the required number of assets for this bucket
                top_bonds = combined_df.head(assets_to_invest - len(asset_type['bonds']))

                # Append to bucket and register symbols as used
                asset_type['bonds'].extend(
                    top_bonds[['Symbol_x', 'Current Yield_x', 'S&P Equivalent_x']].to_dict(orient='records')
                )
                used_symbols.update(top_bonds['Ticker'].tolist())

            # -------------------- Back-fill to reach target counts --------------------
            # Build quick lookup: rating (stripped of +/-) -> bucket reference
            rating_to_bucket = {}
            for bucket in investment_proposal:
                for equiv in bucket['equivalents']:
                    rating_to_bucket[equiv.replace('+', '').replace('-', '')] = bucket

            # Calculate how many more each bucket needs
            bucket_needs = {
                b['name']: int(total_assets * risk_archetype[b['name']]) - len(b['bonds'])
                for b in investment_proposal
            }

            remaining_needed = sum(v for v in bucket_needs.values() if v > 0)

            if remaining_needed > 0:
                # Candidate pool: not yet used tickers, highest yield first, unique per ticker
                remaining_pool = (
                    merged_df[~merged_df['Ticker'].isin(used_symbols)]
                        .sort_values(by='Current Yield_x', ascending=False)
                        .groupby('Ticker')
                        .head(1)
                )

                for _, row in remaining_pool.iterrows():
                    if remaining_needed == 0:
                        break

                    rating_key = str(row['S&P Equivalent_x']).replace('+', '').replace('-', '')
                    bucket = rating_to_bucket.get(rating_key)
                    if not bucket:
                        # default to lowest grade bucket
                        bucket = next(b for b in investment_proposal if b['name'] == 'bonds_bb')

                    if bucket_needs[bucket['name']] <= 0:
                        continue

                    bucket['bonds'].append({
                        'Symbol_x': row['Symbol_x'],
                        'Current Yield_x': row['Current Yield_x'],
                        'S&P Equivalent_x': row['S&P Equivalent_x'],
                    })

                    used_symbols.add(row['Ticker'])
                    bucket_needs[bucket['name']] -= 1
                    remaining_needed -= 1


            for asset_type in investment_proposal:
                logger.announcement(f'Asset Type: {asset_type["name"]}')
                logger.announcement(f'Percentage: {risk_archetype[asset_type["name"]]}')
                logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
                for bond in asset_type['bonds']:
                    logger.info(f'Bond: {bond["Symbol_x"]} - {bond["Current Yield_x"]} - {bond["S&P Equivalent_x"]}')

    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    # Normalize and persist investment proposal to match database schema
    def normalize_bond(record: dict):
        normalized = {
            'symbol': str(record.get('Symbol_x', '')),
            'current_yield': float(record.get('Current Yield_x', 0) or 0),
            'equivalent': str(record.get('S&P Equivalent_x', '')),
        }
        if 'percentage' in record:
            normalized['percentage'] = float(record.get('percentage') or 0)
        return normalized

    def get_bucket(name: str):
        bucket = next((x for x in investment_proposal if x['name'] == name), None)
        return [normalize_bond(b) for b in (bucket['bonds'] if bucket else [])]

    proposal_record = {
        'aaa_a': get_bucket('bonds_aaa_a'),
        'bbb': get_bucket('bonds_bbb'),
        'bb': get_bucket('bonds_bb'),
        'etfs': get_bucket('etfs'),
        'risk_profile_id': risk_profile_id,
    }

    logger.announcement('Saving investment proposal...')
    proposal_id = db.create(table='investment_proposal', data=proposal_record)
    logger.success(f'Investment proposal saved with id: {proposal_id}')

    return proposal_record

@handle_exception
def read_investment_proposals(query: dict = None):
    investment_proposals = db.read(table='investment_proposal', query=query)
    return investment_proposals