import pandas as pd
import json
from src.components.tools.public.reporting import get_open_positions_report, get_proposals_equity_report, get_bond_report, get_ust_bond_report
from src.components.clients.risk_profiles import risk_archetypes, get_risk_archetype_for_score
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger
import numpy as np
import re
import time

TOTAL_ASSETS = 20
INVESTMENT_PROPOSAL_CONTEXT_TTL_SECONDS = 300
_investment_proposal_context_cache: dict | None = None
_investment_proposal_context_cached_at = 0.0

MOODYS_TO_SP_EQUIVALENT = {
    'AAA': 'AAA',
    'AA1': 'AA+',
    'AA2': 'AA',
    'AA3': 'AA-',
    'A1': 'A+',
    'A2': 'A',
    'A3': 'A-',
    'BAA1': 'BBB+',
    'BAA2': 'BBB',
    'BAA3': 'BBB-',
    'BA1': 'BB+',
    'BA2': 'BB',
    'BA3': 'BB-',
    'B1': 'B+',
    'B2': 'B',
    'B3': 'B-',
    'CAA1': 'CCC+',
    'CAA2': 'CCC',
    'CAA3': 'CCC-',
    'CA': 'CC',
    'C': 'C',
}

PROPOSAL_BUCKET_KEYS = {
    'treasuries': 'treasury',
    'bonds_aaa_a': 'aaa_a',
    'bonds_bbb': 'bbb',
    'bonds_bb': 'bb',
    'etfs': 'etfs',
}


def _normalize_rating_token(value: str) -> str:
    token = str(value or '').strip().upper().replace(' ', '')
    return token


def _extract_sp_like_rating_from_text(value: str) -> str:
    text = str(value or '').upper()
    # Prefer explicit S&P-like tokens first.
    candidates = [
        'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-',
        'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC', 'C'
    ]
    for candidate in candidates:
        if candidate in text:
            return candidate
    return ''


def _is_likely_ust_record(row: dict) -> bool:
    joined = ' '.join([
        str(row.get('Issuer', '') or ''),
        str(row.get('Company Name', '') or ''),
        str(row.get('Ticker', '') or ''),
        str(row.get('Symbol_x', '') or ''),
        str(row.get('Financial Instrument', '') or ''),
        str(row.get('Sector', '') or ''),
        str(row.get('Industry', '') or ''),
    ]).upper()

    ust_patterns = [
        r'\bUST\b',
        r'\bTREASURY\b',
        r'\bUNITED STATES TREASURY\b',
        r'\bUS-T\b',
        r'\bT-?NOTE\b',
        r'\bT-?BOND\b',
        r'\bU\.?S\.?\s+GOVT\b',
    ]
    return any(re.search(pattern, joined) for pattern in ust_patterns)


def _resolve_rating(row: dict) -> str:
    if _is_likely_ust_record(row):
        return 'UST'

    sp_equivalent = _normalize_rating_token(row.get('S&P Equivalent'))
    if sp_equivalent:
        return sp_equivalent

    sp = _normalize_rating_token(row.get('SP'))
    if sp:
        return sp

    ratings_text = _extract_sp_like_rating_from_text(row.get('Ratings'))
    if ratings_text:
        return ratings_text

    moodys_raw = _normalize_rating_token(row.get('Moodys'))
    if moodys_raw:
        mapped = MOODYS_TO_SP_EQUIVALENT.get(moodys_raw)
        if mapped:
            return mapped

    return ''


def _to_float_or_none(value):
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip().replace('%', '')
        if cleaned == '':
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_yield_percent(value: float | None) -> float:
    if value is None:
        return 0.0
    # If value is fractional (e.g., 0.045), convert to percent points (4.5).
    if 0 <= value <= 1:
        return round(value * 100, 4)
    return round(value, 4)


def _resolve_current_yield_percent(row: dict) -> float:
    # Priority: explicit Current Yield, then CY, then YTM.
    current_yield = _to_float_or_none(row.get('Current Yield'))
    if current_yield is not None:
        return _normalize_yield_percent(current_yield)

    cy = _to_float_or_none(row.get('CY'))
    if cy is not None:
        return _normalize_yield_percent(cy)

    ytm = _to_float_or_none(row.get('YTM'))
    if ytm is not None:
        return _normalize_yield_percent(ytm)

    return 0.0


def _build_investment_proposal_template() -> list[dict]:
    return [
        {
            'name': 'treasuries',
            'equivalents': ['UST'],
            'bonds': []
        },
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


def _get_bucket_for_rating(rating: str) -> str:
    normalized = str(rating).strip().upper().replace('+', '').replace('-', '')
    if normalized == 'UST':
        return 'treasuries'
    if normalized == 'ETF':
        return 'etfs'
    if normalized in {'AAA', 'AA', 'A'}:
        return 'bonds_aaa_a'
    if normalized == 'BBB':
        return 'bonds_bbb'
    if normalized == 'BB':
        return 'bonds_bb'
    return ''


def _load_investment_proposal_context() -> dict:
    global _investment_proposal_context_cache
    global _investment_proposal_context_cached_at

    now = time.monotonic()
    cache_is_fresh = (
        _investment_proposal_context_cache is not None
        and (now - _investment_proposal_context_cached_at) < INVESTMENT_PROPOSAL_CONTEXT_TTL_SECONDS
    )
    if cache_is_fresh:
        logger.info('Using cached investment proposal context.')
        return _investment_proposal_context_cache

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
    rtd_report = get_bond_report()
    rtd_df = pd.DataFrame(rtd_report)

    # Get UST bonds report
    ust_report = get_ust_bond_report()
    ust_df = pd.DataFrame(ust_report)

    # Remove IBCID Symbol column if it exists and clean it (though not strictly used for merge anymore)
    if 'Symbol' in rtd_df.columns:
        # logger.announcement(rtd_df['Symbol'].head(10))
        pass

    if 'Symbol' in ust_df.columns:
        # logger.announcement(ust_df['Symbol'].head(10))
        pass

    # Build merged bond universe for symbol validation (RTD + UST)
    merged_universe_df = pd.concat([rtd_df.copy(), ust_df.copy()], ignore_index=True)
    if 'Symbol' in merged_universe_df.columns:
        merged_universe_df['Symbol'] = merged_universe_df['Symbol'].astype(str).str.strip()
        merged_universe_df = merged_universe_df[merged_universe_df['Symbol'] != '']
        merged_universe_df = merged_universe_df.drop_duplicates(subset=['Symbol'], keep='first')

    logger.announcement(f'Total bonds from RTD: {len(rtd_df)}')
    logger.announcement(f'Total bonds from UST: {len(ust_df)}')
    logger.announcement(f'Total bonds from merged universe: {len(merged_universe_df)}')

    # Use the combined corporate + UST universe as the base for candidates so
    # treasury allocations can actually populate the treasury bucket.
    merged_df = pd.concat([rtd_df.copy(), ust_df.copy()], ignore_index=True)

    # Rename columns to match expected format (using _x suffix as legacy from previous merge)
    merged_df = merged_df.rename(columns={
        'Financial Instrument': 'Symbol_x',
        'Current Yield': 'Current Yield_x',
        'S&P Equivalent': 'S&P Equivalent_x',
        'Issuer': 'Ticker'  # Or derived below
    })

    logger.announcement(f'Total bonds from RTD: {len(merged_df)}')
    logger.info(f"Merged DF columns: {merged_df.columns.tolist()}")

    if 'S&P Equivalent_x' in merged_df.columns:
        missing_equivalent_mask = merged_df['S&P Equivalent_x'].astype(str).str.strip() == ''
        if missing_equivalent_mask.any():
            merged_df.loc[missing_equivalent_mask, 'S&P Equivalent_x'] = merged_df[missing_equivalent_mask].apply(
                lambda row: _resolve_rating(row.to_dict()),
                axis=1,
            )

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

    merged_df['Current Yield_x'] = merged_df['Current Yield_x'].apply(
        lambda value: _normalize_yield_percent(_to_float_or_none(value))
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

    context = {
        'bonds_df_no_duplicates': bonds_df_no_duplicates,
        'avg_yield': avg_yield,
        'rtd_df': rtd_df,
        'merged_universe_df': merged_universe_df,
        'merged_df': merged_df
    }
    _investment_proposal_context_cache = context
    _investment_proposal_context_cached_at = now
    logger.info(f'Cached investment proposal context for {INVESTMENT_PROPOSAL_CONTEXT_TTL_SECONDS} seconds.')
    return context


def _normalize_distribution(distribution: dict) -> dict:
    cleaned = {key: float(value or 0) for key, value in distribution.items()}
    total = sum(cleaned.values())
    if total > 0 and not np.isclose(total, 1.0):
        cleaned = {key: value / total for key, value in cleaned.items()}
    return cleaned


def _distribution_from_assets(investment_proposal: list[dict]) -> dict:
    distribution = {}
    for bucket in investment_proposal:
        bucket_total = sum(float(bond.get('percentage') or 0) for bond in bucket.get('bonds', []))
        distribution[bucket['name']] = bucket_total
    return _normalize_distribution(distribution)


def _empty_assets_payload() -> dict:
    return {key: [] for key in PROPOSAL_BUCKET_KEYS.values()}


def _assets_from_investment_proposal(investment_proposal: list[dict]) -> dict:
    assets = _empty_assets_payload()
    for bucket_name, proposal_key in PROPOSAL_BUCKET_KEYS.items():
        bucket = next((bucket for bucket in investment_proposal if bucket['name'] == bucket_name), None)
        assets[proposal_key] = [_normalize_bond_record(bond) for bond in (bucket.get('bonds', []) if bucket else [])]
    return assets


def _assets_from_saved_proposal(proposal: dict) -> dict:
    raw_assets = proposal.get('assets')
    if isinstance(raw_assets, str):
        try:
            raw_assets = json.loads(raw_assets)
        except Exception:
            raw_assets = None

    if isinstance(raw_assets, dict):
        assets = _empty_assets_payload()
        for key in assets:
            bucket_assets = raw_assets.get(key) or []
            assets[key] = [_normalize_bond_record(asset) for asset in bucket_assets if isinstance(asset, dict)]
        return assets

    # Legacy fallback for rows that still have the old columns before migration is applied.
    legacy_assets = _empty_assets_payload()
    for bucket_name, proposal_key in PROPOSAL_BUCKET_KEYS.items():
        legacy_assets[proposal_key] = [
            _normalize_bond_record(asset)
            for asset in (proposal.get(proposal_key) or [])
            if isinstance(asset, dict)
        ]
    return legacy_assets


def _distribution_from_saved_assets_payload(assets: dict) -> dict | None:
    if not isinstance(assets, dict):
        return None

    raw_distribution = {
        'treasuries': len(assets.get('treasury') or []),
        'bonds_aaa_a': len(assets.get('aaa_a') or []),
        'bonds_bbb': len(assets.get('bbb') or []),
        'bonds_bb': len(assets.get('bb') or []),
        'etfs': len(assets.get('etfs') or []),
    }

    if sum(raw_distribution.values()) == 0:
        return None

    return _normalize_distribution(raw_distribution)


def _normalize_planner_inputs(planner_inputs: dict | None) -> dict | None:
    if not isinstance(planner_inputs, dict):
        return None

    return {
        'risk_profile_id': planner_inputs.get('risk_profile_id'),
        'name': planner_inputs.get('name'),
        'target_return': planner_inputs.get('target_return'),
        'starting_amount': planner_inputs.get('starting_amount'),
        'risk_tolerance': planner_inputs.get('risk_tolerance'),
        'selected_risk_archetype': planner_inputs.get('selected_risk_archetype'),
        'allocation': planner_inputs.get('allocation') or {},
        'bond_rating_allocation': planner_inputs.get('bond_rating_allocation') or {},
        'locked_assets': planner_inputs.get('locked_assets') or {},
        'locked_bond_ratings': planner_inputs.get('locked_bond_ratings') or {},
    }


def _distribution_from_risk_archetype(risk_archetype: dict) -> dict:
    distribution = {
        'treasuries': risk_archetype.get('treasuries', 0),
        'bonds_aaa_a': risk_archetype.get('bonds_aaa_a', 0),
        'bonds_bbb': risk_archetype.get('bonds_bbb', 0),
        'bonds_bb': risk_archetype.get('bonds_bb', 0),
        'etfs': risk_archetype.get('etfs', 0),
    }
    return _normalize_distribution(distribution)


def _js_round(value: float) -> int:
    return int(np.floor(float(value) + 0.5))


def _default_allocation_from_risk_archetype(risk_archetype: dict) -> dict:
    bonds = _js_round((
        float(risk_archetype.get('bonds_aaa_a', 0) or 0)
        + float(risk_archetype.get('bonds_bbb', 0) or 0)
        + float(risk_archetype.get('bonds_bb', 0) or 0)
    ) * 100)

    return {
        'cash': 0,
        'treasuries': _js_round(float(risk_archetype.get('treasuries', 0) or 0) * 100),
        'bonds': bonds,
        'stocks': _js_round(float(risk_archetype.get('etfs', 0) or 0) * 100),
    }


def _default_bond_rating_allocation_from_risk_archetype(risk_archetype: dict) -> dict:
    total_bond_share = (
        float(risk_archetype.get('bonds_aaa_a', 0) or 0)
        + float(risk_archetype.get('bonds_bbb', 0) or 0)
        + float(risk_archetype.get('bonds_bb', 0) or 0)
    )

    if total_bond_share <= 0:
        return {'aaa': 0, 'bbb': 50, 'bb': 50}

    aaa = _js_round((float(risk_archetype.get('bonds_aaa_a', 0) or 0) / total_bond_share) * 100)
    bbb = _js_round((float(risk_archetype.get('bonds_bbb', 0) or 0) / total_bond_share) * 100)
    bb = max(0, 100 - aaa - bbb)
    return {'aaa': aaa, 'bbb': bbb, 'bb': bb}


def _portfolio_plan_matches_selected_archetype_defaults(portfolio_plan: dict, risk_archetype: dict) -> bool:
    allocation = portfolio_plan.get('allocation') or {}
    bond_rating_allocation = portfolio_plan.get('bond_rating_allocation') or {}

    expected_allocation = _default_allocation_from_risk_archetype(risk_archetype)
    expected_bond_rating_allocation = _default_bond_rating_allocation_from_risk_archetype(risk_archetype)

    allocation_matches = all(
        float(allocation.get(key) or 0) == float(expected_allocation.get(key) or 0)
        for key in expected_allocation
    )
    bond_rating_matches = all(
        float(bond_rating_allocation.get(key) or 0) == float(expected_bond_rating_allocation.get(key) or 0)
        for key in expected_bond_rating_allocation
    )

    return allocation_matches and bond_rating_matches


def _distribution_from_portfolio_plan(portfolio_plan: dict) -> dict:
    selected_archetype_name = str(portfolio_plan.get('selected_risk_archetype') or '').strip()
    if selected_archetype_name:
        selected_archetype = next(
            (risk_archetype for risk_archetype in risk_archetypes if str(risk_archetype.get('name') or '').strip() == selected_archetype_name),
            None,
        )
        if selected_archetype and _portfolio_plan_matches_selected_archetype_defaults(portfolio_plan, selected_archetype):
            return _distribution_from_risk_archetype(selected_archetype)

    allocation = portfolio_plan.get('allocation') or {}
    bond_rating_allocation = portfolio_plan.get('bond_rating_allocation') or {}

    cash = float(allocation.get('cash') or 0) / 100
    treasuries = float(allocation.get('treasuries') or 0) / 100
    bonds = float(allocation.get('bonds') or 0) / 100
    stocks = float(allocation.get('stocks') or 0) / 100

    aaa = float(bond_rating_allocation.get('aaa') or 0) / 100
    bbb = float(bond_rating_allocation.get('bbb') or 0) / 100
    bb = float(bond_rating_allocation.get('bb') or 0) / 100

    distribution = {
        'treasuries': cash + treasuries,
        'bonds_aaa_a': bonds * aaa,
        'bonds_bbb': bonds * bbb,
        'bonds_bb': bonds * bb,
        'etfs': stocks,
    }
    return _normalize_distribution(distribution)


def _initialize_used_symbols(bonds_df_no_duplicates: pd.DataFrame) -> set[str]:
    used_symbols: set[str] = set()
    if not bonds_df_no_duplicates.empty and 'Symbol_x' in bonds_df_no_duplicates.columns:
        existing_tickers = bonds_df_no_duplicates['Symbol_x'].astype(str).str.strip().str.split().str[0].tolist()
        used_symbols.update(existing_tickers)
        logger.info(f"Initialized used_symbols with {len(used_symbols)} existing tickers.")
    return used_symbols


def _populate_investment_proposal_from_distribution(
    investment_proposal: list[dict],
    distribution: dict,
    context: dict,
):
    bonds_df_no_duplicates = context['bonds_df_no_duplicates']
    avg_yield = context['avg_yield']
    merged_df = context['merged_df']
    normalized_distribution = _normalize_distribution(distribution)

    used_symbols = _initialize_used_symbols(bonds_df_no_duplicates)

    for asset_type in investment_proposal:
        percentage = float(normalized_distribution.get(asset_type['name'], 0) or 0)
        assets_to_invest = int(round(TOTAL_ASSETS * percentage))

        logger.info(f"--- Processing bucket: {asset_type['name']} ---")
        logger.info(f"Distribution percentage: {percentage}")
        logger.info(f"Assets to invest: {assets_to_invest}")
        logger.info(f"Current bonds in bucket: {len(asset_type['bonds'])}")

        if asset_type['name'] == 'etfs' and assets_to_invest > 0:
            asset_type['bonds'].append({
                'Symbol_x': 'SPY',
                'Current Yield_x': float(avg_yield) if avg_yield is not None else 0.0,
                'S&P Equivalent_x': 'ETF',
            })
            used_symbols.add('SPY')

        sanitized_equivalents = asset_type['equivalents']
        if asset_type['name'] == 'treasuries':
            combined_df = merged_df[
                merged_df.apply(lambda row: _resolve_rating(row.to_dict()) == 'UST', axis=1)
            ]
        else:
            combined_df = merged_df[
                merged_df['S&P Equivalent_x']
                    .astype(str)
                    .str.replace(r'[+\-]', '', regex=True)
                    .isin(sanitized_equivalents)
            ]

        combined_df = (
            combined_df
                .sort_values(by='Current Yield_x', ascending=False)
                .groupby('Ticker')
                .head(1)
        )

        combined_df = combined_df[~combined_df['Ticker'].isin(used_symbols)]
        top_bonds = combined_df.head(max(0, assets_to_invest - len(asset_type['bonds'])))

        normalized_top_bonds = []
        for _, row in top_bonds.iterrows():
            normalized_top_bonds.append({
                'Symbol_x': row['Symbol_x'],
                'Current Yield_x': row['Current Yield_x'],
                'S&P Equivalent_x': _resolve_rating(row.to_dict()) or row['S&P Equivalent_x'],
            })

        asset_type['bonds'].extend(normalized_top_bonds)
        used_symbols.update(top_bonds['Ticker'].tolist())

    rating_to_bucket = {}
    for bucket in investment_proposal:
        for equiv in bucket['equivalents']:
            rating_to_bucket[equiv.replace('+', '').replace('-', '')] = bucket

    bucket_needs = {
        bucket['name']: max(0, int(round(TOTAL_ASSETS * float(normalized_distribution.get(bucket['name'], 0) or 0))) - len(bucket['bonds']))
        for bucket in investment_proposal
    }

    remaining_needed = sum(bucket_needs.values())

    if remaining_needed > 0:
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
                bucket = next(bucket_ref for bucket_ref in investment_proposal if bucket_ref['name'] == 'bonds_bb')

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
        logger.announcement(f'Percentage: {normalized_distribution.get(asset_type["name"], 0)}')
        logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
        for bond in asset_type['bonds']:
            logger.info(f'Bond: {bond["Symbol_x"]} - {bond["Current Yield_x"]} - {bond["S&P Equivalent_x"]}')


def _persist_investment_proposal(
    investment_proposal: list[dict],
    risk_profile_id,
    source_type: str,
    planner_inputs: dict | None = None,
):
    proposal_record = _serialize_investment_proposal(
        investment_proposal=investment_proposal,
        risk_profile_id=risk_profile_id,
        source_type=source_type,
        planner_inputs=planner_inputs,
    )

    logger.announcement('Saving investment proposal...')
    existing_proposals = []
    if source_type == 'hub_original' and risk_profile_id:
        matching_risk_profile_proposals = db.read(table='investment_proposal', query={'risk_profile_id': risk_profile_id}) or []
        existing_proposals = [
            proposal for proposal in matching_risk_profile_proposals
            if str(proposal.get('source_type') or '').strip() == source_type
        ]

    existing_proposals = sorted(
        existing_proposals,
        key=lambda proposal: str(proposal.get('created') or ''),
        reverse=True,
    )

    if existing_proposals:
        proposal_id = db.update(
            table='investment_proposal',
            query={'id': existing_proposals[0]['id']},
            data=proposal_record,
        )
        logger.success(f'Investment proposal updated with id: {proposal_id}')
    else:
        proposal_id = db.create(table='investment_proposal', data=proposal_record)
        logger.success(f'Investment proposal saved with id: {proposal_id}')

    saved_proposals = db.read(table='investment_proposal', query={'id': proposal_id}) or []
    if saved_proposals:
        return _normalize_saved_investment_proposal(saved_proposals[0])

    return _normalize_saved_investment_proposal({'id': proposal_id, **proposal_record})


def _normalize_bond_record(record: dict):
    normalized = {
        'symbol': str(record.get('symbol', record.get('Symbol_x', ''))),
        'current_yield': float(record.get('current_yield', record.get('Current Yield_x', 0)) or 0),
        'equivalent': str(record.get('equivalent', record.get('S&P Equivalent_x', ''))),
    }
    if 'percentage' in record:
        normalized['percentage'] = float(record.get('percentage') or 0)
    return normalized


def _serialize_investment_proposal(
    investment_proposal: list[dict],
    risk_profile_id,
    source_type: str,
    planner_inputs: dict | None = None,
):
    return {
        'assets': _assets_from_investment_proposal(investment_proposal),
        'risk_profile_id': risk_profile_id,
        'source_type': source_type,
        'planner_inputs': _normalize_planner_inputs(planner_inputs),
    }


def _average_bucket_yield(bonds: list[dict]) -> float:
    if not bonds:
        return 0.0
    return sum(float(bond.get('current_yield') or 0) for bond in bonds) / len(bonds)


def _build_investment_proposal_preview(
    investment_proposal: list[dict],
    risk_profile_id,
    distribution: dict,
    planner_inputs: dict | None = None,
):
    normalized_distribution = _normalize_distribution(distribution)
    proposal_record = _serialize_investment_proposal(
        investment_proposal=investment_proposal,
        risk_profile_id=risk_profile_id,
        source_type='planner',
        planner_inputs=planner_inputs,
    )

    bucket_mapping = [
        ('treasury', 'treasuries'),
        ('aaa_a', 'bonds_aaa_a'),
        ('bbb', 'bonds_bbb'),
        ('bb', 'bonds_bb'),
        ('etfs', 'etfs'),
    ]

    bucket_summaries = []
    expected_average_yield = 0.0
    total_assets = 0

    for record_key, distribution_key in bucket_mapping:
        bonds = (proposal_record.get('assets') or {}).get(record_key, [])
        average_yield = _average_bucket_yield(bonds)
        weight = float(normalized_distribution.get(distribution_key, 0) or 0)
        total_assets += len(bonds)
        expected_average_yield += average_yield * weight
        bucket_summaries.append({
            'key': distribution_key,
            'weight': weight,
            'asset_count': len(bonds),
            'average_yield': average_yield,
        })

    return {
        **proposal_record,
        'derived_distribution': normalized_distribution,
        'total_assets': total_assets,
        'bucket_summaries': bucket_summaries,
        'expected_average_yield': round(expected_average_yield, 6),
        'expected_return_decimal': round(expected_average_yield / 100, 8),
    }


def _derive_distribution_for_saved_proposal(proposal: dict) -> dict | None:
    source_type = str(proposal.get('source_type') or '').strip()
    risk_profile_id = proposal.get('risk_profile_id')

    if source_type == 'planner':
        normalized_planner_inputs = _normalize_planner_inputs(proposal.get('planner_inputs'))
        if normalized_planner_inputs:
            return _distribution_from_portfolio_plan(normalized_planner_inputs)
        derived_from_assets = _distribution_from_saved_assets_payload(_assets_from_saved_proposal(proposal))
        if derived_from_assets:
            return derived_from_assets

    if risk_profile_id:
        risk_profiles = db.read(table='risk_profile', query={'id': risk_profile_id}) or []
        if risk_profiles:
            risk_archetype = get_risk_archetype_for_score(risk_profiles[0].get('score'))
            if risk_archetype:
                return _distribution_from_risk_archetype(risk_archetype)

    return None


def _normalize_saved_investment_proposal(proposal: dict) -> dict:
    normalized_source_type = str(proposal.get('source_type') or '').strip()
    if normalized_source_type not in {'hub_original', 'planner', 'custom'}:
        normalized_source_type = 'hub_original'

    return {
        **proposal,
        'source_type': normalized_source_type,
        'assets': _assets_from_saved_proposal(proposal),
        'planner_inputs': _normalize_planner_inputs(proposal.get('planner_inputs')),
        'derived_distribution': _derive_distribution_for_saved_proposal({**proposal, 'source_type': normalized_source_type}),
    }

@handle_exception
def create_investment_proposal_with_assets(assets: list[dict]):
    logger.announcement('Generating investment proposal from assets...')

    try:
        context = _load_investment_proposal_context()
        merged_universe_df = context['merged_universe_df']
        investment_proposal = _build_investment_proposal_template()

        if not isinstance(assets, list):
            raise Exception('assets must be a list of dicts with symbol and percentage.')

        logger.announcement(f'Creating proposal from {len(assets)} assets.')

        raw_percentages = []
        for asset in assets:
            if not isinstance(asset, dict):
                raise Exception('Each asset must be a dict with symbol and percentage.')
            percentage = asset.get('percentage')
            if percentage is None:
                raise Exception('Each asset must include symbol and percentage.')
            raw_percentages.append(float(percentage))

        total_percentage = sum(raw_percentages)
        if total_percentage <= 0:
            raise Exception('Asset percentages must include at least one positive value.')

        # Normalize to 1.0; accept either 0-1 fractions or 0-100 percentages
        assets = [{**asset} for asset in assets]
        if total_percentage > 1.5:
            for asset in assets:
                asset['percentage'] = float(asset.get('percentage', 0)) / 100.0
            total_percentage = sum(float(asset.get('percentage', 0)) for asset in assets)

        if not np.isclose(total_percentage, 1.0) and total_percentage > 0:
            for asset in assets:
                asset['percentage'] = float(asset.get('percentage', 0)) / total_percentage

        for asset in assets:
            if not isinstance(asset, dict):
                raise Exception('Each asset must be a dict with symbol and percentage.')

            symbol = asset.get('symbol')
            percentage = asset.get('percentage')

            if not symbol or percentage is None:
                raise Exception('Each asset must include symbol and percentage.')

            normalized_symbol = str(symbol).strip()
            universe_match = merged_universe_df[merged_universe_df['Symbol'] == normalized_symbol]
            if universe_match.empty:
                raise Exception(f'IBCID "{symbol}" not found in bond universe (RTD + UST).')

            rtd_row = universe_match.iloc[0].to_dict()
            rating = _resolve_rating(rtd_row)
            if not rating:
                raise Exception(
                    f'No rating found in bond universe for IBCID "{symbol}". '
                    f'Fields: SP="{rtd_row.get("SP", "")}", '
                    f'S&P Equivalent="{rtd_row.get("S&P Equivalent", "")}", '
                    f'Ratings="{rtd_row.get("Ratings", "")}", '
                    f'Moodys="{rtd_row.get("Moodys", "")}".'
                )

            bucket_name = _get_bucket_for_rating(rating)
            if not bucket_name:
                raise Exception(f'Unknown rating "{rating}" for asset {symbol}.')

            bucket = next(b for b in investment_proposal if b['name'] == bucket_name)
            current_yield_pct = _resolve_current_yield_percent(rtd_row)
            bucket['bonds'].append({
                'Symbol_x': str(rtd_row.get('Financial Instrument') or symbol),
                'Current Yield_x': current_yield_pct,
                'S&P Equivalent_x': str(rating),
                'ibcid': str(symbol),
                'percentage': float(percentage),
            })
            print(f'Bucket: {bucket_name}')

        for asset_type in investment_proposal:
            logger.announcement(f'Asset Type: {asset_type["name"]}')
            logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
            for bond in asset_type['bonds']:
                logger.info(
                    f'Bond: {bond["Symbol_x"]} - {bond.get("percentage", 0)} - {bond["S&P Equivalent_x"]}'
                )
    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    return _persist_investment_proposal(investment_proposal, None, 'custom')


@handle_exception
def create_investment_proposal_with_risk_profile(risk_profile: dict):
    logger.announcement('Generating investment proposal from risk profile...')

    try:
        if not risk_profile:
            raise Exception('Risk profile is required when assets are not provided.')

        context = _load_investment_proposal_context()
        investment_proposal = _build_investment_proposal_template()

        logger.announcement(f'Risk profile: {risk_profile}')
        risk_score = risk_profile['score']
        risk_profile_id = risk_profile['id']
        risk_archetype = get_risk_archetype_for_score(risk_score)
        if not risk_archetype:
            logger.error(f'Risk profile with score {risk_score} not found')
            raise Exception(f'Risk profile with score {risk_score} not found')

        distribution = _distribution_from_risk_archetype(risk_archetype)
        _populate_investment_proposal_from_distribution(
            investment_proposal=investment_proposal,
            distribution=distribution,
            context=context,
        )

    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    return _persist_investment_proposal(investment_proposal, risk_profile_id, 'hub_original')


@handle_exception
def create_investment_proposal_with_portfolio_plan(portfolio_plan: dict):
    logger.announcement('Generating investment proposal from portfolio plan...')

    try:
        if not portfolio_plan:
            raise Exception('Portfolio plan is required when generating an investment proposal from a plan.')

        context = _load_investment_proposal_context()
        investment_proposal = _build_investment_proposal_template()
        distribution = _distribution_from_portfolio_plan(portfolio_plan)
        risk_profile_id = portfolio_plan.get('risk_profile_id')

        _populate_investment_proposal_from_distribution(
            investment_proposal=investment_proposal,
            distribution=distribution,
            context=context,
        )

    except Exception as exc:
        logger.error(f'Failed creating investment proposal from plan: {exc}')
        raise Exception(f'Failed creating investment proposal from plan: {exc}')

    return _persist_investment_proposal(
        investment_proposal,
        risk_profile_id,
        'planner',
        planner_inputs=portfolio_plan,
    )


@handle_exception
def preview_investment_proposal_with_portfolio_plan(portfolio_plan: dict):
    logger.announcement('Previewing investment proposal from portfolio plan...')

    try:
        if not portfolio_plan:
            raise Exception('Portfolio plan is required when previewing an investment proposal from a plan.')

        context = _load_investment_proposal_context()
        investment_proposal = _build_investment_proposal_template()
        distribution = _distribution_from_portfolio_plan(portfolio_plan)
        risk_profile_id = portfolio_plan.get('risk_profile_id')

        _populate_investment_proposal_from_distribution(
            investment_proposal=investment_proposal,
            distribution=distribution,
            context=context,
        )
    except Exception as exc:
        logger.error(f'Failed previewing investment proposal from plan: {exc}')
        raise Exception(f'Failed previewing investment proposal from plan: {exc}')

    return _build_investment_proposal_preview(
        investment_proposal,
        risk_profile_id,
        distribution,
        planner_inputs=portfolio_plan,
    )

@handle_exception
def read_investment_proposals(query: dict = None):
    investment_proposals = db.read(table='investment_proposal', query=query)
    return [_normalize_saved_investment_proposal(investment_proposal) for investment_proposal in investment_proposals]
