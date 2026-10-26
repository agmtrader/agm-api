import pandas as pd
import json
from src.components.tools.public.reporting import (
    get_bond_report,
    get_etfs_report,
    get_open_positions_report,
    get_proposals_equity_report,
    get_stocks_report,
    get_ust_bond_report,
)
from src.components.clients.risk_profiles import risk_archetypes, get_risk_archetype_for_score
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError, handle_exception
from src.utils.logger import logger
import numpy as np
import re
import time
import math
import heapq
from sqlalchemy import case, func
from itertools import count

# Equity ETFs in the market snapshot. New symbols require classification before
# entering this sleeve; fixed-income and commodity ETFs stay out of equity.
EQUITY_ETF_SYMBOLS = frozenset({
    'EEM', 'DIA', 'XLP', 'VEA', 'VT', 'XLY', 'XLE', 'ARKK', 'QQQ', 'XLRE',
    'IEMG', 'IYR', 'EWJ', 'VOO', 'IBB', 'XLB', 'VTI', 'XLV', 'QQQM', 'RSP',
    'VNQ', 'IWM', 'VXUS', 'IVV', 'XLK', 'SOXX', 'XLU', 'EFA', 'SPY', 'VWO',
    'IEFA', 'XLF', 'XLI', 'ACWI', 'SMH', 'SCHB',
})
# Reviewed breadth/concentration preferences, not measured fund-quality ratings.
# Similar index exposures share a family so additional wrappers lose priority.
# Source references and the limits of these proxies are recorded in PROC-005.
EQUITY_ETF_SELECTION_POLICY = {
    'VT': (1.00, 'global'), 'ACWI': (0.98, 'global'),
    'VTI': (1.00, 'us_market'), 'SCHB': (0.99, 'us_market'),
    'VOO': (0.98, 'us_market'), 'IVV': (0.98, 'us_market'),
    'SPY': (0.98, 'us_market'),
    'VXUS': (0.97, 'international'),
    'VEA': (0.96, 'developed'), 'IEFA': (0.95, 'developed'),
    'EFA': (0.94, 'developed'),
    'RSP': (0.94, 'us_equal_weight'), 'DIA': (0.90, 'us_blue_chip'),
    'IWM': (0.88, 'us_small_cap'),
    'IEMG': (0.88, 'emerging'), 'VWO': (0.88, 'emerging'),
    'EEM': (0.85, 'emerging'), 'EWJ': (0.85, 'japan'),
    'QQQM': (0.86, 'nasdaq_growth'), 'QQQ': (0.85, 'nasdaq_growth'),
    'XLP': (0.72, 'staples'), 'XLV': (0.72, 'healthcare'),
    'XLU': (0.72, 'utilities'), 'XLI': (0.70, 'industrials'),
    'XLF': (0.68, 'financials'), 'XLB': (0.68, 'materials'),
    'XLY': (0.65, 'consumer_discretionary'), 'XLE': (0.62, 'energy'),
    'XLK': (0.62, 'technology'),
    'VNQ': (0.65, 'real_estate'), 'IYR': (0.62, 'real_estate'),
    'XLRE': (0.62, 'real_estate'),
    'SOXX': (0.45, 'semiconductors'), 'SMH': (0.45, 'semiconductors'),
    'IBB': (0.45, 'biotechnology'), 'ARKK': (0.35, 'innovation_theme'),
}
ETF_PLANNING_RETURN_CENTER = 10.0
ETF_BREADTH_SCORE_WEIGHT = 0.8
ETF_SAME_EXPOSURE_PENALTY = 0.75
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
    'cash': 'cash',
    'treasuries': 'treasury',
    'bonds_aaa_a': 'aaa_a',
    'bonds_bbb': 'bbb',
    'bonds_bb': 'bb',
    'investment_grade': 'investment_grade',
    'bonds': 'bonds',
    'stocks': 'stocks',
    'etfs': 'etfs',
}

ASSET_METADATA_FIELDS = (
    'industry',
    'coupon',
    'years_to_maturity',
    'duration',
    'maturity',
)

SP_RATINGS = (
    'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
    'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-',
    'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'SD', 'D',
)


def _normalize_rating_token(value) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip().upper().replace(' ', '').replace('−', '-').replace('–', '-')


def _validated_sp_rating(value) -> str:
    token = _normalize_rating_token(value)
    return token if token in SP_RATINGS or token == 'UST' else ''


def _extract_sp_like_rating_from_text(value) -> str:
    if value is None or pd.isna(value):
        return ''
    text = str(value).upper().replace('−', '-').replace('–', '-')
    text = re.sub(r'\bN\s*/\s*A\b', '', text)
    # Match whole rating tokens, never letters embedded in agency names or words.
    tokens = sorted(set(SP_RATINGS) | set(MOODYS_TO_SP_EQUIVALENT), key=len, reverse=True)
    pattern = r'(?<![A-Z0-9+\-])(' + '|'.join(re.escape(token) for token in tokens) + r')(?![A-Z0-9+\-])'
    ratings = [MOODYS_TO_SP_EQUIVALENT.get(token, token) for token in re.findall(pattern, text)]
    # Like the market-data ETL, retain the lower credit rating when agencies differ.
    return max(ratings, key=SP_RATINGS.index) if ratings else ''


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

    # Report merges rename the canonical ETL equivalent with an _x suffix.
    # Preserve a valid canonical rating, including ratings outside proposal buckets.
    for field in ('S&P Equivalent_x', 'S&P Equivalent'):
        equivalent = _validated_sp_rating(row.get(field))
        if equivalent:
            return equivalent

    sp = _validated_sp_rating(row.get('SP'))
    moodys = MOODYS_TO_SP_EQUIVALENT.get(_normalize_rating_token(row.get('Moodys')), '')
    structured = [rating for rating in (sp, moodys) if rating]
    if structured:
        return max(structured, key=lambda rating: SP_RATINGS.index(rating) if rating != 'UST' else -1)

    return _extract_sp_like_rating_from_text(row.get('Ratings'))


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

def _get_string_field(row: dict, keys: list[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if text and text.lower() not in {'nan', 'nat', 'none', 'null'}:
            return text
    return ''


def _get_number_field(row: dict, keys: list[str]) -> float | None:
    for key in keys:
        value = _to_float_or_none(row.get(key))
        if value is not None and np.isfinite(value):
            return float(value)
    return None


def _resolve_asset_metadata(row: dict) -> dict:
    return {
        'industry': _get_string_field(row, ['industry', 'Industry', 'sector', 'Sector']),
        'coupon': _get_number_field(row, ['coupon', 'Coupon']),
        'years_to_maturity': _get_number_field(
            row,
            ['years_to_maturity', 'Years to Maturity', 'yearsToMaturity'],
        ),
        'duration': _get_number_field(row, ['duration', 'Duration']),
        'maturity': _get_string_field(row, ['maturity', 'Maturity']),
    }

def _resolve_market_asset_symbol(row: dict) -> str:
    return _get_string_field(row, ['Symbol', 'symbol', 'Ticker', 'ticker', 'sheet_name'])

def _resolve_market_asset_display_symbol(row: dict, fallback_symbol: str) -> str:
    return _get_string_field(
        row,
        ['Financial Instrument', 'financialInstrument', 'Ticker', 'ticker', 'Symbol', 'symbol', 'sheet_name'],
    ) or fallback_symbol

def _resolve_equity_yield_percent(row: dict) -> float:
    for key in [
        'Current Yield',
        'current_yield',
        'Dividend Yield',
        'dividend_yield',
        'Yield(UserInput)',
        'Average Annual Return',
        'average_annual_return',
        'Yield',
        'yield',
        'YTM',
        'ytm',
    ]:
        value = _to_float_or_none(row.get(key))
        if value is not None:
            return _normalize_yield_percent(value)
    return 0.0


def _median_equity_yield_percent(rows) -> float:
    values = [
        _resolve_equity_yield_percent(row)
        for row in rows
        if isinstance(row, dict)
    ]
    values = [value for value in values if value > 0]
    return float(np.median(values)) if values else 0.0

def _find_matching_market_row(df: pd.DataFrame, symbol: str) -> dict | None:
    normalized_symbol = str(symbol or '').strip().upper()
    if not normalized_symbol or df.empty:
        return None

    for column in [
        'Symbol', 'symbol', 'Ticker', 'ticker', 'sheet_name',
        'Financial Instrument', 'financialInstrument',
    ]:
        if column not in df.columns:
            continue

        matches = df[
            df[column]
                .astype(str)
                .str.strip()
                .str.upper()
                == normalized_symbol
        ]
        if not matches.empty:
            return matches.iloc[0].to_dict()

    return None

def _resolve_source_bucket(asset: dict) -> str:
    source_bucket = str(asset.get('source_bucket') or asset.get('source') or asset.get('asset_class') or '').strip().upper()
    return source_bucket

def _build_investment_proposal_template() -> list[dict]:
    return [
        {'name': 'treasuries', 'equivalents': ['UST'], 'bonds': []},
        {'name': 'investment_grade', 'equivalents': ['AAA', 'AA', 'A', 'BBB'], 'bonds': []},
        {'name': 'bonds_bb', 'equivalents': ['BB'], 'bonds': []},
        {'name': 'etfs', 'equivalents': ['ETF'], 'bonds': []},
    ]


def _get_bucket_for_rating(rating: str) -> str:
    token = _normalize_rating_token(rating)
    normalized = token if token in {'UST', 'ETF'} else _validated_sp_rating(token).replace('+', '').replace('-', '')
    return {'UST': 'treasuries', 'ETF': 'etfs', 'AAA': 'investment_grade',
            'AA': 'investment_grade', 'A': 'investment_grade', 'BBB': 'investment_grade', 'BB': 'bonds_bb'}.get(normalized, '')


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

    # Get RTD report
    rtd_report = get_bond_report()
    rtd_df = pd.DataFrame(rtd_report)

    # Get UST bonds report
    ust_report = get_ust_bond_report()
    ust_df = pd.DataFrame(ust_report)

    stocks_report = get_stocks_report()
    stocks_df = pd.DataFrame(stocks_report)

    etfs_report = get_etfs_report()
    etfs_df = pd.DataFrame(etfs_report)

    # Some currently-uploaded stock/ETF snapshots predate the five-year
    # Current Yield column. Keep proposal previews usable while those files
    # are refreshed by deriving a temporary equity-yield fallback from the
    # existing proposal-equity feed.
    proposal_equity_report = get_proposals_equity_report()
    proposal_equity_df = pd.DataFrame(proposal_equity_report)
    proposal_equity_yield_pct = _median_equity_yield_percent(
        proposal_equity_df.to_dict(orient='records')
    )

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

    merged_df['S&P Equivalent_x'] = merged_df.apply(lambda row: _resolve_rating(row.to_dict()), axis=1)

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
        'rtd_df': rtd_df,
        'ust_df': ust_df,
        'stocks_df': stocks_df,
        'etfs_df': etfs_df,
        'proposal_equity_df': proposal_equity_df,
        'proposal_equity_yield_pct': proposal_equity_yield_pct,
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
    assets['bonds'] = [
        *assets.get('aaa_a', []),
        *assets.get('bbb', []),
        *assets.get('bb', []),
        *assets.get('investment_grade', []),
    ]
    return assets


def _canonical_proposal_assets(assets: dict) -> dict:
    """Remove retired holdings and expose consistent capital weights on read."""
    assets = {**assets}
    removed = bool(assets.pop('gold', []))
    for key, holdings in assets.items():
        retained = [asset for asset in holdings if not _is_retired_proposal_asset(asset)]
        removed = removed or len(retained) != len(holdings)
        assets[key] = retained
    assets['investment_grade'] = [*assets.get('investment_grade', []), *assets.get('aaa_a', []), *assets.get('bbb', [])]
    assets['aaa_a'] = []
    assets['bbb'] = []
    canonical_keys = ('cash', 'treasury', 'investment_grade', 'bb', 'stocks', 'etfs')
    holdings = [asset for key in canonical_keys for asset in assets.get(key, [])]
    if removed and holdings and all(asset.get('percentage') is not None for asset in holdings):
        total = sum(float(asset['percentage']) for asset in holdings)
        if total > 0:
            for key in canonical_keys:
                assets[key] = [{**asset, 'percentage': float(asset['percentage']) / total}
                               for asset in assets.get(key, [])]
    assets['bonds'] = [*assets['investment_grade'], *assets.get('bb', [])]
    return assets


def _is_retired_proposal_asset(asset: dict) -> bool:
    return (str(asset.get('symbol') or '').strip().upper() in {'GLD', 'IAU'}
            or str(asset.get('equivalent') or '').strip().upper() == 'GOLD')


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
        assets['gold'] = raw_assets.get('gold') or []
        if not any(key in raw_assets for key in ('cash', 'bonds', 'stocks')):
            assets['bonds'] = [*assets.get('aaa_a', []), *assets.get('bbb', []), *assets.get('bb', [])]
        return _canonical_proposal_assets(assets)

    # Legacy fallback for rows that still have the old columns before migration is applied.
    legacy_assets = _empty_assets_payload()
    for bucket_name, proposal_key in PROPOSAL_BUCKET_KEYS.items():
        legacy_assets[proposal_key] = [
            _normalize_bond_record(asset)
            for asset in (proposal.get(proposal_key) or [])
            if isinstance(asset, dict)
        ]
    legacy_assets['gold'] = proposal.get('gold') or []
    return _canonical_proposal_assets(legacy_assets)


def _asset_metadata_value_is_missing(field: str, value) -> bool:
    if field in {'industry', 'maturity'}:
        return not str(value or '').strip()
    numeric_value = _to_float_or_none(value)
    return numeric_value is None or not np.isfinite(numeric_value)


def _assets_need_market_metadata(assets: dict) -> bool:
    for key in ('treasury', 'aaa_a', 'bbb', 'bb', 'investment_grade', 'bonds'):
        for asset in assets.get(key, []) or []:
            if isinstance(asset, dict) and any(
                _asset_metadata_value_is_missing(field, asset.get(field))
                for field in ASSET_METADATA_FIELDS
            ):
                return True
    return False


def _enrich_assets_with_market_metadata(assets: dict, context: dict) -> dict:
    enriched_assets = {**assets}
    bucket_sources = {
        'treasury': context['ust_df'],
        'aaa_a': context['rtd_df'],
        'bbb': context['rtd_df'],
        'bb': context['rtd_df'],
        'bonds': context['rtd_df'],
        'investment_grade': context['rtd_df'],
    }

    for key, market_df in bucket_sources.items():
        enriched_bucket = []
        for asset in assets.get(key, []) or []:
            if not isinstance(asset, dict):
                continue

            enriched_asset = {**asset}
            if any(
                _asset_metadata_value_is_missing(field, enriched_asset.get(field))
                for field in ASSET_METADATA_FIELDS
            ):
                market_row = _find_matching_market_row(market_df, enriched_asset.get('symbol'))
                if market_row:
                    market_metadata = _resolve_asset_metadata(market_row)
                    for field in ASSET_METADATA_FIELDS:
                        if (
                            _asset_metadata_value_is_missing(field, enriched_asset.get(field))
                            and not _asset_metadata_value_is_missing(field, market_metadata.get(field))
                        ):
                            enriched_asset[field] = market_metadata[field]

            enriched_bucket.append(enriched_asset)
        enriched_assets[key] = enriched_bucket

    return enriched_assets


def _distribution_from_saved_assets_payload(assets: dict) -> dict | None:
    if not isinstance(assets, dict):
        return None

    bucket_assets = {
        'cash': assets.get('cash') or [],
        'stocks': assets.get('stocks') or [],
        'treasuries': assets.get('treasury') or [],
        'bonds_aaa_a': assets.get('aaa_a') or [],
        'bonds_bbb': assets.get('bbb') or [],
        'bonds_bb': assets.get('bb') or [],
        'etfs': assets.get('etfs') or [],
        'investment_grade': assets.get('investment_grade') or [],
    }

    # Asset percentages are the proposal's actual allocation. Do not infer
    # allocation from the number of selected securities: a custom proposal
    # can contain one ETF at 20% and many bonds at 5% each.
    percentage_distribution = {
        key: sum(
            float(asset.get('percentage') or 0)
            for asset in bucket
            if isinstance(asset, dict) and asset.get('percentage') is not None
        )
        for key, bucket in bucket_assets.items()
    }

    if sum(percentage_distribution.values()) > 0:
        return _normalize_distribution(percentage_distribution)

    # Compatibility fallback for older proposals whose assets did not store
    # percentages.
    raw_distribution = {key: len(bucket) for key, bucket in bucket_assets.items()}

    if sum(raw_distribution.values()) == 0:
        return None

    return _normalize_distribution(raw_distribution)


def _clean_candidate_pool(candidate_df: pd.DataFrame) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df

    cleaned = candidate_df.copy()
    cleaned = cleaned[cleaned['Ticker'].astype(str).str.strip() != '']
    cleaned = cleaned[cleaned['Current Yield_x'].notna()]
    cleaned = cleaned[cleaned['Current Yield_x'] > 0]
    cleaned = cleaned[cleaned['Current Yield_x'] <= 25]
    return cleaned.reset_index(drop=True)


def _bucket_selection_profile(candidate_df: pd.DataFrame) -> dict | None:
    if candidate_df.empty:
        return None

    yields = candidate_df['Current Yield_x'].astype(float)
    median_yield = float(yields.median())
    mad = float((yields - median_yield).abs().median())
    mad_floor = max(mad, 0.35)
    lower_bound = max(0.0, median_yield - (2.5 * mad_floor))
    upper_bound = min(float(yields.quantile(0.90)), median_yield + (2.5 * mad_floor))
    target_yield = min(float(yields.quantile(0.75)), median_yield + (1.25 * mad_floor))
    spread = max(upper_bound - lower_bound, 1.0)

    return {
        'median_yield': median_yield,
        'mad': mad_floor,
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'target_yield': target_yield,
        'spread': spread,
    }


def _screen_outliers_for_bucket(candidate_df: pd.DataFrame, selection_profile: dict | None) -> pd.DataFrame:
    if candidate_df.empty or not selection_profile:
        return candidate_df

    screened = candidate_df[
        (candidate_df['Current Yield_x'] >= selection_profile['lower_bound'])
        & (candidate_df['Current Yield_x'] <= selection_profile['upper_bound'])
    ]

    if len(screened) >= max(3, min(5, len(candidate_df))):
        return screened.reset_index(drop=True)
    return candidate_df.reset_index(drop=True)


def _score_candidates_for_bucket(candidate_df: pd.DataFrame, selection_profile: dict | None) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df

    if not selection_profile:
        scored = candidate_df.copy()
        scored['_selector_score'] = 0.0
        return scored

    target_yield = float(selection_profile['target_yield'])
    spread = float(selection_profile['spread'])

    scored = candidate_df.copy()
    scored['_selector_score'] = scored['Current Yield_x'].apply(
        lambda candidate_yield: max(0.0, 1.0 - (abs(float(candidate_yield) - target_yield) / spread))
    )
    scored['_distance_to_target'] = scored['Current Yield_x'].apply(
        lambda candidate_yield: abs(float(candidate_yield) - target_yield)
    )

    scored = scored.sort_values(
        by=['_selector_score', '_distance_to_target', 'Current Yield_x'],
        ascending=[False, True, False],
    )
    return scored


def _prepare_bucket_candidates(
    candidate_df: pd.DataFrame,
    used_symbols: set[str],
) -> tuple[pd.DataFrame, dict | None]:
    cleaned_candidates = _clean_candidate_pool(candidate_df)
    selection_profile = _bucket_selection_profile(cleaned_candidates)
    screened_candidates = _screen_outliers_for_bucket(cleaned_candidates, selection_profile)
    rescored_profile = _bucket_selection_profile(screened_candidates) or selection_profile
    ranked_candidates = _score_candidates_for_bucket(screened_candidates, rescored_profile)
    ranked_candidates = ranked_candidates.groupby('Ticker').head(1)
    ranked_candidates = ranked_candidates[~ranked_candidates['Ticker'].isin(used_symbols)]
    return ranked_candidates, rescored_profile


def _prepare_etf_candidates(etfs_df: pd.DataFrame, proposal_equity_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Adapt observed ETF returns without inventing missing per-fund history."""
    if etfs_df.empty:
        return pd.DataFrame(columns=['Ticker', 'Symbol_x', 'Current Yield_x', 'S&P Equivalent_x'])

    candidates = etfs_df.copy()
    display_symbol = (
        candidates.get('Financial Instrument', candidates.get('Symbol', pd.Series(index=candidates.index)))
        .astype(str)
        .str.strip()
        .str.upper()
    )
    candidates['Ticker'] = display_symbol
    candidates['Symbol_x'] = display_symbol
    raw_yields = candidates.get('Current Yield', pd.Series(None, index=candidates.index, dtype=object))
    parsed_yields = raw_yields.apply(_to_float_or_none)
    missing_yields = parsed_yields.apply(lambda value: value is None or not np.isfinite(value))
    candidates['Current Yield_x'] = parsed_yields
    if missing_yields.any():
        logger.warning(
            f'ETF snapshot has {int(missing_yields.sum())} missing Current Yield value(s); '
            'excluding those candidates until per-fund history is available.'
        )
        candidates = candidates.loc[~missing_yields].copy()
    candidates['S&P Equivalent_x'] = 'ETF'
    # Keep the source columns so optional proposal metadata such as industry
    # can be preserved when it exists in the ETF snapshot.
    return candidates


def _rank_equity_etf_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    """Prefer breadth, modest return assumptions and distinct index exposures.

    The 8-12% planning range has a soft 10% center, never a return filter or
    a replacement for the observed return. Breadth owns 80% of the base score.
    This ordering is reused by the count optimizer, not just the first sort.
    """
    remaining = [row for row in candidates.to_dict(orient='records')
                 if row['Ticker'] in EQUITY_ETF_SELECTION_POLICY]
    ranked = []
    exposure_counts = {}

    def score(row):
        breadth, exposure = EQUITY_ETF_SELECTION_POLICY[row['Ticker']]
        return_fit = 1 / (1 + abs(float(row['Current Yield_x']) - ETF_PLANNING_RETURN_CENTER) / 5)
        base = 10 * (ETF_BREADTH_SCORE_WEIGHT * breadth + (1 - ETF_BREADTH_SCORE_WEIGHT) * return_fit)
        return base - ETF_SAME_EXPOSURE_PENALTY * exposure_counts.get(exposure, 0)

    while remaining:
        remaining.sort(key=lambda row: (-score(row), row['Ticker']))
        selected = remaining.pop(0)
        selected['_selection_score'] = score(selected)
        exposure = EQUITY_ETF_SELECTION_POLICY[selected['Ticker']][1]
        exposure_counts[exposure] = exposure_counts.get(exposure, 0) + 1
        ranked.append(selected)
    return pd.DataFrame(ranked, columns=[*candidates.columns, '_selection_score'])


def _distribution_from_risk_archetype(risk_archetype: dict) -> dict:
    return {
        'treasuries': risk_archetype['treasuries'],
        'investment_grade': risk_archetype['investment_grade'],
        'bonds_bb': risk_archetype['high_yield'],
        'etfs': risk_archetype['equity'],
    }


def _js_round(value: float) -> int:
    return int(np.floor(float(value) + 0.5))


def _default_allocation_from_risk_archetype(risk_archetype: dict) -> dict:
    keys = ('treasuries', 'investment_grade', 'high_yield', 'equity')
    total = sum(risk_archetype[key] for key in keys)
    raw = {key: risk_archetype[key] / total * 10000 for key in keys}
    units = {key: math.floor(raw[key] + 1e-9) for key in keys}
    ordered = sorted(keys, key=lambda key: raw[key] - units[key], reverse=True)
    for key in ordered[:10000 - sum(units.values())]:
        units[key] += 1
    return {key: units[key] / 100 for key in keys}


def _portfolio_plan_matches_selected_archetype_defaults(portfolio_plan: dict, risk_archetype: dict) -> bool:
    allocation = portfolio_plan.get('allocation') or {}
    expected = _default_allocation_from_risk_archetype(risk_archetype)
    return set(allocation) == set(expected) and all(
        math.isclose(float(allocation[key]), value, rel_tol=0, abs_tol=1e-8)
        for key, value in expected.items()
    )


def _distribution_from_portfolio_plan(portfolio_plan: dict) -> dict:
    allocation = portfolio_plan.get('allocation') or {}
    mapping = {'treasuries': 'treasuries', 'investment_grade': 'investment_grade',
               'high_yield': 'bonds_bb', 'equity': 'etfs'}
    if not isinstance(allocation, dict) or set(allocation) != set(mapping):
        raise ServiceError('Use Treasuries, Investment grade, High yield and Equity allocations.',
                           status_code=422, code='invalid_proposal_allocation')
    try:
        values = {key: float(allocation[key]) for key in mapping}
    except (ValueError, TypeError):
        raise ServiceError('Allocations must be finite nonnegative percentages totaling 100%.',
                           status_code=422, code='invalid_proposal_allocation')
    if any(not math.isfinite(value) or value < 0 for value in values.values()) or not math.isclose(sum(values.values()), 100, abs_tol=1e-8):
        raise ServiceError('Allocations must be finite nonnegative percentages totaling 100%.',
                           status_code=422, code='invalid_proposal_allocation')
    return {mapping[key]: value / 100 for key, value in values.items()}


def _position_limit_for_portfolio_plan(portfolio_plan: dict) -> float | None:
    archetype = next(
        (item for item in risk_archetypes if item['name'] == portfolio_plan.get('selected_risk_archetype')),
        None,
    )
    if archetype and _portfolio_plan_matches_selected_archetype_defaults(portfolio_plan, archetype):
        return archetype['largest_position']
    return None


def _total_assets_for_portfolio_plan(portfolio_plan: dict) -> int:
    value = portfolio_plan.get('total_assets')
    if value is None or (isinstance(value, str) and not value.strip()):
        return TOTAL_ASSETS
    return value


def _initialize_used_symbols(bonds_df_no_duplicates: pd.DataFrame) -> set[str]:
    used_symbols: set[str] = set()
    if not bonds_df_no_duplicates.empty and 'Symbol_x' in bonds_df_no_duplicates.columns:
        existing_tickers = bonds_df_no_duplicates['Symbol_x'].astype(str).str.strip().str.split().str[0].tolist()
        used_symbols.update(existing_tickers)
        logger.info(f"Initialized used_symbols with {len(used_symbols)} existing tickers.")
    return used_symbols


def _optimize_bucket_asset_selection(pools: list[list[dict]], weights: list[float], total_assets: int, max_position: float | None = None):
    """Maximize weighted selection scores with exact count and unique issuers.

    The independent-sleeve optimum bounds each search branch. Only branch when
    its selected holdings conflict, so unused candidates never reserve issuers.
    """
    def solve(excluded):
        available = [[row for row in rows if row['Ticker'] not in excluded[i]][:total_assets]
                     for i, rows in enumerate(pools)]
        if len({row['Ticker'] for rows in available for row in rows}) < total_assets:
            return None
        states = {0: (0.0, [])}
        for weight, rows in zip(weights, available):
            next_states = {}
            prefix = np.cumsum([float(row.get('_selection_score', row['Current Yield_x'])) for row in rows])
            for size, (score, counts) in states.items():
                minimum_count = max(1, math.ceil(weight / max_position - 1e-10)) if max_position else 1
                for k in range(minimum_count, min(len(rows), total_assets - size) + 1):
                    candidate = score + weight * float(prefix[k - 1]) / k
                    if size + k not in next_states or candidate > next_states[size + k][0]:
                        next_states[size + k] = (candidate, counts + [k])
            states = next_states
        if total_assets not in states:
            return None
        score, counts = states[total_assets]
        return score, [rows[:k] for rows, k in zip(available, counts)]

    excluded = tuple(frozenset() for _ in pools)
    initial = solve(excluded)
    if initial is None:
        return None
    sequence = count()
    queue = [(-initial[0], next(sequence), excluded, initial[1])]
    visited = {excluded}
    while queue:
        _, _, excluded, selected = heapq.heappop(queue)
        seen = {}
        conflict = None
        for i, rows in enumerate(selected):
            for row in rows:
                issuer = row['Ticker']
                if issuer in seen:
                    conflict = (issuer, seen[issuer], i)
                    break
                seen[issuer] = i
            if conflict:
                break
        if not conflict:
            return selected  # Best bound is now feasible: global optimum.
        issuer, first, second = conflict
        for i in (first, second):
            child = tuple(value | {issuer} if j == i else value for j, value in enumerate(excluded))
            if child in visited:
                continue
            visited.add(child)
            result = solve(child)
            if result is not None:
                heapq.heappush(queue, (-result[0], next(sequence), child, result[1]))
    return None


def _populate_investment_proposal_from_distribution(
    investment_proposal: list[dict], distribution: dict, context: dict,
    total_assets: int = TOTAL_ASSETS, max_position: float | None = None,
):
    if isinstance(total_assets, bool) or not isinstance(total_assets, int) or total_assets < 1:
        raise ServiceError('Total assets must be a positive integer.', status_code=400, code='invalid_asset_count')
    weights = _normalize_distribution(distribution)
    active = [bucket for bucket in investment_proposal if weights.get(bucket['name'], 0) > 0]
    if total_assets < len(active):
        raise ServiceError('Total assets must cover every nonzero allocation class.', status_code=422, code='invalid_asset_count')
    merged = context['merged_df'].copy()
    merged['S&P Equivalent_x'] = merged.apply(lambda row: _resolve_rating(row.to_dict()), axis=1)
    used = _initialize_used_symbols(context['bonds_df_no_duplicates'])
    pools = []
    for bucket in investment_proposal:
        bucket['bonds'] = []
    for bucket in active:
        if bucket['name'] == 'etfs':
            etfs = context['etfs_df']
            symbols = etfs.apply(lambda row: _resolve_market_asset_display_symbol(row.to_dict(), '').upper(), axis=1)
            candidates = _prepare_etf_candidates(etfs[symbols.isin(EQUITY_ETF_SYMBOLS)], context.get('proposal_equity_df'))
        else:
            equivalents = bucket['equivalents']
            candidates = merged[merged['S&P Equivalent_x'].astype(str).str.replace(r'[+\-]', '', regex=True).isin(equivalents)]
        if bucket['name'] == 'etfs':
            cleaned = candidates[candidates['Current Yield_x'].apply(lambda value: value is not None and np.isfinite(value))]
        else:
            cleaned = _clean_candidate_pool(candidates)
        if bucket['name'] == 'treasuries':
            cleaned = cleaned.copy()
            cleaned['Ticker'] = cleaned['Symbol_x']

        screened = cleaned if bucket['name'] == 'etfs' else _screen_outliers_for_bucket(cleaned, _bucket_selection_profile(cleaned))
        eligible = screened.sort_values(['Current Yield_x', 'Ticker'], ascending=[False, True]).groupby('Ticker').head(1)
        eligible = eligible[~eligible['Ticker'].isin(used)]
        ranked = (_rank_equity_etf_candidates(eligible) if bucket['name'] == 'etfs'
                  else eligible)
        rows = ranked.to_dict(orient='records')
        if not rows:
            raise ServiceError(f"Unable to generate investment proposal: no eligible assets for {bucket['name']}.", status_code=422, code='proposal_assets_unavailable', details={'missing_buckets': [bucket['name']]})
        pools.append(rows)
    selected = _optimize_bucket_asset_selection(pools, [weights[bucket['name']] for bucket in active], total_assets, max_position)
    if selected is None:
        raise ServiceError(f'Cannot build exactly {total_assets} distinct eligible assets. Reduce the total or refresh market data.', status_code=422, code='proposal_asset_count_unavailable')
    for bucket, rows in zip(active, selected):
        for row in rows:
            bucket['bonds'].append({
                'Symbol_x': row['Symbol_x'], 'Current Yield_x': row['Current Yield_x'],
                'S&P Equivalent_x': _resolve_rating(row) or row['S&P Equivalent_x'],
                'percentage': weights[bucket['name']] / len(rows),
                **_resolve_asset_metadata(row),
            })


def _persist_investment_proposal(
    investment_proposal: list[dict],
    risk_profile_id,
    source_type: str,
    contact_id=None,
    starting_amount=None,
):
    if risk_profile_id and not contact_id:
        linked_profiles = db.read(table='risk_profile', query={'id': risk_profile_id}) or []
        if linked_profiles:
            contact_id = linked_profiles[0].get('contact_id')

    proposal_record = _serialize_investment_proposal(
        investment_proposal=investment_proposal,
        risk_profile_id=risk_profile_id,
        source_type=source_type,
        contact_id=contact_id,
        starting_amount=starting_amount,
    )

    logger.announcement('Saving investment proposal...')
    # Every generation is a new immutable proposal record. A planner run must
    # never overwrite the original risk-profile proposal.
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
        **_resolve_asset_metadata(record),
    }
    if 'percentage' in record:
        normalized['percentage'] = float(record.get('percentage') or 0)
    return normalized


def _serialize_investment_proposal(
    investment_proposal: list[dict],
    risk_profile_id,
    source_type: str,
    contact_id=None,
    starting_amount=None,
):
    return {
        'assets': _assets_from_investment_proposal(investment_proposal),
        'risk_profile_id': risk_profile_id,
        'contact_id': contact_id,
        'source_type': source_type,
        'starting_amount': starting_amount,
    }


def _average_bucket_yield(bonds: list[dict]) -> float:
    if not bonds:
        return 0.0
    weight = sum(float(bond.get('percentage') or 0) for bond in bonds)
    if weight > 0 and all(bond.get('percentage') is not None for bond in bonds):
        return sum(float(bond['percentage']) * float(bond.get('current_yield') or 0) for bond in bonds) / weight
    return sum(float(bond.get('current_yield') or 0) for bond in bonds) / len(bonds)


def _build_investment_proposal_preview(
    investment_proposal: list[dict],
    risk_profile_id,
    distribution: dict,
    starting_amount=None,
    source_type='portfolio_plan',
):
    normalized_distribution = _normalize_distribution(distribution)
    proposal_record = _serialize_investment_proposal(
        investment_proposal=investment_proposal,
        risk_profile_id=risk_profile_id,
        source_type=source_type,
        starting_amount=starting_amount,
    )

    proposal_record['assets'] = _assets_from_saved_proposal(proposal_record)
    bucket_mapping = [
        ('cash', 'cash'),
        ('stocks', 'stocks'),
        ('treasury', 'treasuries'),
        ('aaa_a', 'bonds_aaa_a'),
        ('bbb', 'bonds_bbb'),
        ('bb', 'bonds_bb'),
        ('etfs', 'etfs'),
        ('investment_grade', 'investment_grade'),
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
        if weight <= 0:
            continue
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
    risk_profile_id = proposal.get('risk_profile_id')

    # The proposal's saved assets are authoritative for every source type.
    # Risk archetypes are only a compatibility fallback for legacy rows that
    # have no persisted assets.
    saved_assets = _assets_from_saved_proposal(proposal)
    derived_from_assets = _distribution_from_saved_assets_payload(saved_assets)
    if derived_from_assets:
        return derived_from_assets
    if proposal.get('assets') and not any(saved_assets.values()):
        return None

    if risk_profile_id:
        risk_profiles = db.read(table='risk_profile', query={'id': risk_profile_id}) or []
        if risk_profiles:
            risk_archetype = get_risk_archetype_for_score(risk_profiles[0].get('score'))
            if risk_archetype:
                return _distribution_from_risk_archetype(risk_archetype)

    return None


def _normalize_saved_investment_proposal(proposal: dict) -> dict:
    normalized_source_type = str(proposal.get('source_type') or '').strip()
    if normalized_source_type not in {'risk_profile', 'portfolio_plan', 'custom'}:
        normalized_source_type = 'risk_profile'

    raw_starting_amount = proposal.get('starting_amount')
    try:
        normalized_starting_amount = float(raw_starting_amount) if raw_starting_amount is not None else None
    except (TypeError, ValueError):
        normalized_starting_amount = None

    return {
        **{key: value for key, value in proposal.items() if key != 'gold'},
        'source_type': normalized_source_type,
        'starting_amount': normalized_starting_amount,
        'assets': _assets_from_saved_proposal(proposal),
        'derived_distribution': _derive_distribution_for_saved_proposal({**proposal, 'source_type': normalized_source_type}),
    }

def _build_investment_proposal_with_assets(assets: list[dict]):
    logger.announcement('Generating investment proposal from assets...')

    try:
        context = _load_investment_proposal_context()
        rtd_df = context['rtd_df']
        ust_df = context['ust_df']
        etfs_df = context['etfs_df']
        investment_proposal = _build_investment_proposal_template()

        if not isinstance(assets, list):
            raise Exception('assets must be a list of dicts with symbol, percentage, and source_bucket.')

        logger.announcement(f'Creating proposal from {len(assets)} assets.')

        raw_percentages = []
        for asset in assets:
            if not isinstance(asset, dict):
                raise Exception('Each asset must be a dict with symbol and percentage.')
            percentage = asset.get('percentage')
            if percentage is None:
                raise Exception('Each asset must include symbol and percentage.')
            value = float(percentage)
            if not math.isfinite(value) or value < 0:
                raise ServiceError('Asset weights must be finite and nonnegative.', status_code=400, code='invalid_asset_weight')
            raw_percentages.append(value)

        total_percentage = sum(raw_percentages)
        if total_percentage <= 0:
            raise Exception('Asset percentages must include at least one positive value.')

        # Normalize to 1.0; accept either 0-1 fractions or 0-100 percentages
        assets = [{**asset} for asset in assets if float(asset['percentage']) > 0]
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
            source_bucket = _resolve_source_bucket(asset)

            if not symbol or percentage is None:
                raise Exception('Each asset must include symbol and percentage.')

            normalized_symbol = str(symbol).strip().upper()
            matched_row = None
            bucket_name = ''
            rating = ''
            current_yield_pct = 0.0

            allowed_sources = {'TREASURIES', 'INVESTMENT_GRADE', 'HIGH_YIELD', 'EQUITY'}
            if source_bucket not in allowed_sources:
                raise ServiceError('Choose a Treasuries, Investment grade, High yield or Equity ETF holding.',
                                   status_code=422, code='invalid_proposal_asset_class')
            if source_bucket == 'TREASURIES':
                matched_row = _find_matching_market_row(ust_df, normalized_symbol)
                bucket_name, rating = 'treasuries', 'UST'
                if matched_row:
                    current_yield_pct = _resolve_current_yield_percent(matched_row)
            elif source_bucket in {'INVESTMENT_GRADE', 'HIGH_YIELD'}:
                matched_row = _find_matching_market_row(rtd_df, normalized_symbol)
                if matched_row:
                    rating = _resolve_rating(matched_row)
                    bucket_name = _get_bucket_for_rating(rating)
                    current_yield_pct = _resolve_current_yield_percent(matched_row)
                    expected_bucket = 'investment_grade' if source_bucket == 'INVESTMENT_GRADE' else 'bonds_bb'
                    if bucket_name != expected_bucket:
                        raise ServiceError('The bond rating does not match the selected proposal category.',
                                           status_code=422, code='invalid_proposal_asset_class')
            else:
                matched_row = _find_matching_market_row(etfs_df, normalized_symbol)
                if matched_row:
                    ticker = _resolve_market_asset_display_symbol(matched_row, normalized_symbol).upper()
                    if ticker in EQUITY_ETF_SYMBOLS:
                        bucket_name, rating = 'etfs', 'ETF'
                    else:
                        raise ServiceError('Equity accepts classified equity ETFs only.',
                                           status_code=422, code='invalid_proposal_asset_class')
                    current_yield_pct = _to_float_or_none(matched_row.get('Current Yield'))
                    if current_yield_pct is None and source_bucket == 'EQUITY':
                        current_yield_pct = float(context.get('proposal_equity_yield_pct') or 0)
                    if current_yield_pct is None or not math.isfinite(current_yield_pct):
                        raise ServiceError('The selected holding has no usable source return.',
                                           status_code=422, code='proposal_assets_unavailable')
            if not matched_row:
                raise ServiceError('The selected holding was not found in its proposal category feed.',
                                   status_code=422, code='proposal_assets_unavailable')
            if any(bond.get('ibcid') == normalized_symbol for item in investment_proposal for bond in item['bonds']):
                raise ServiceError('Each holding may appear only once in a proposal.',
                                   status_code=422, code='duplicate_proposal_asset')

            bucket = next(b for b in investment_proposal if b['name'] == bucket_name)
            bucket['bonds'].append({
                'Symbol_x': _resolve_market_asset_display_symbol(matched_row, normalized_symbol),
                'Current Yield_x': current_yield_pct,
                'S&P Equivalent_x': str(rating),
                **_resolve_asset_metadata(matched_row),
                'ibcid': normalized_symbol,
                'percentage': float(percentage),
                'source_bucket': source_bucket,
            })
            print(f'Bucket: {bucket_name}')

        for asset_type in investment_proposal:
            logger.announcement(f'Asset Type: {asset_type["name"]}')
            logger.announcement(f'Assets to invest: {len(asset_type["bonds"])}')
            for bond in asset_type['bonds']:
                logger.info(
                    f'Bond: {bond["Symbol_x"]} - {bond.get("percentage", 0)} - {bond["S&P Equivalent_x"]}'
                )
    except ServiceError:
        raise
    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    return investment_proposal


@handle_exception
def create_investment_proposal_with_assets(assets: list[dict], risk_profile_id=None, contact_id=None, starting_amount=None):
    investment_proposal = _build_investment_proposal_with_assets(assets)
    return _persist_investment_proposal(investment_proposal, risk_profile_id, 'custom', contact_id=contact_id, starting_amount=starting_amount)


@handle_exception
def preview_investment_proposal_with_assets(assets: list[dict], risk_profile_id=None, starting_amount=None):
    investment_proposal = _build_investment_proposal_with_assets(assets)
    distribution = _distribution_from_saved_assets_payload(_assets_from_investment_proposal(investment_proposal))
    return _build_investment_proposal_preview(investment_proposal, risk_profile_id, distribution, starting_amount, source_type='custom')


@handle_exception
def create_investment_proposal_with_risk_profile(risk_profile: dict, starting_amount=None):
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
            total_assets=TOTAL_ASSETS,
            max_position=risk_archetype['largest_position'],
        )

    except ServiceError:
        raise
    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    return _persist_investment_proposal(investment_proposal, risk_profile_id, 'risk_profile', starting_amount=starting_amount)


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
            total_assets=_total_assets_for_portfolio_plan(portfolio_plan),
            max_position=_position_limit_for_portfolio_plan(portfolio_plan),
        )

    except ServiceError:
        raise
    except Exception as exc:
        logger.error(f'Failed creating investment proposal from plan: {exc}')
        raise Exception(f'Failed creating investment proposal from plan: {exc}')

    return _persist_investment_proposal(
        investment_proposal,
        risk_profile_id,
        'portfolio_plan',
        contact_id=portfolio_plan.get('contact_id'),
        starting_amount=portfolio_plan.get('starting_amount'),
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
            total_assets=_total_assets_for_portfolio_plan(portfolio_plan),
            max_position=_position_limit_for_portfolio_plan(portfolio_plan),
        )
    except ServiceError:
        raise
    except Exception as exc:
        logger.error(f'Failed previewing investment proposal from plan: {exc}')
        raise Exception(f'Failed previewing investment proposal from plan: {exc}')

    return _build_investment_proposal_preview(
        investment_proposal,
        risk_profile_id,
        distribution,
        starting_amount=portfolio_plan.get('starting_amount'),
    )

@handle_exception
def read_investment_proposals(query: dict = None, view: str = 'full'):
    if view not in ('full', 'list'):
        raise ServiceError('view must be full or list', status_code=400)
    if view == 'list':
        # Count JSON objects in SQL instead of transporting/enriching every asset.
        # Preserve legacy JSON strings for the existing tolerant normalizer.
        assets = db.metadata.tables['investment_proposal'].c.assets
        keys = ('cash', 'treasury', 'bonds', 'aaa_a', 'bbb', 'bb', 'stocks', 'etfs', 'investment_grade')
        counts = func.jsonb_build_object(*[
            value for key in keys for value in (
                key, case((func.jsonb_typeof(assets[key]) == 'array',
                    func.jsonb_array_length(func.jsonb_path_query_array(
                        assets,
                        f'$.{key}[*] ? (@.type() == "object" && !('
                        'exists(@.symbol ? (@ like_regex "^(GLD|IAU)$" flag "i")) || '
                        'exists(@.Symbol_x ? (@ like_regex "^(GLD|IAU)$" flag "i")) || '
                        'exists(@.equivalent ? (@ like_regex "^GOLD$" flag "i")) || '
                        'exists(@."S&P Equivalent_x" ? (@ like_regex "^GOLD$" flag "i"))))'
                    ))), else_=0),
            )
        ])
        projected_assets = case(
            (func.jsonb_typeof(assets) == 'string', assets),
            else_=func.jsonb_build_object('counts', counts, 'canonical',
                assets.has_key('cash') | assets.has_key('bonds') | assets.has_key('stocks')),
        ).label('assets')
        rows = db.read(table='investment_proposal', query=query, column_overrides={'assets': projected_assets})
        summaries = []
        for row in rows:
            raw = row.get('assets')
            if isinstance(raw, dict) and 'counts' in raw:
                selected = ('cash', 'treasury', 'bonds', 'stocks', 'etfs') if raw.get('canonical') else ('cash', 'treasury', 'aaa_a', 'bbb', 'bb', 'stocks', 'etfs', 'investment_grade')
                total = sum((raw.get('counts') or {}).get(key, 0) or 0 for key in selected)
            else:
                normalized = _assets_from_saved_proposal(row)
                total = sum(len(normalized.get(key) or []) for key in ('cash', 'treasury', 'bonds', 'stocks', 'etfs'))
            summary = {key: value for key, value in row.items() if key != 'assets'}
            summary['source_type'] = row.get('source_type') if row.get('source_type') in ('risk_profile', 'portfolio_plan', 'custom') else 'risk_profile'
            summaries.append({**summary, 'total_assets': total})
        return summaries
    investment_proposals = db.read(table='investment_proposal', query=query)
    normalized_proposals = [
        _normalize_saved_investment_proposal(investment_proposal)
        for investment_proposal in investment_proposals
    ]

    if not any(
        _assets_need_market_metadata(proposal.get('assets') or {})
        for proposal in normalized_proposals
    ):
        return normalized_proposals

    try:
        context = _load_investment_proposal_context()
    except Exception as exc:
        logger.warning(f'Unable to enrich saved proposal asset metadata: {exc}')
        return normalized_proposals

    return [
        {
            **proposal,
            'assets': _enrich_assets_with_market_metadata(proposal.get('assets') or {}, context),
        }
        for proposal in normalized_proposals
    ]
