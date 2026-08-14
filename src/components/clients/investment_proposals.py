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
    'cash': 'cash',
    'treasuries': 'treasury',
    'bonds_aaa_a': 'aaa_a',
    'bonds_bbb': 'bbb',
    'bonds_bb': 'bb',
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
        {'name': 'cash', 'equivalents': ['CASH'], 'bonds': []},
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
        },
        {'name': 'stocks', 'equivalents': ['STOCK'], 'bonds': []},
        {'name': 'bonds', 'equivalents': [], 'bonds': []},
    ]

def _get_bucket_for_rating(rating: str) -> str:
    normalized = str(rating).strip().upper().replace('+', '').replace('-', '')
    if normalized == 'UST':
        return 'treasuries'
    if normalized == 'ETF':
        return 'etfs'
    if normalized == 'STOCK':
        return 'stocks'
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
    ]
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
        if not any(key in raw_assets for key in ('cash', 'bonds', 'stocks')):
            assets['bonds'] = [*assets.get('aaa_a', []), *assets.get('bbb', []), *assets.get('bb', [])]
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


def _asset_metadata_value_is_missing(field: str, value) -> bool:
    if field in {'industry', 'maturity'}:
        return not str(value or '').strip()
    numeric_value = _to_float_or_none(value)
    return numeric_value is None or not np.isfinite(numeric_value)


def _assets_need_market_metadata(assets: dict) -> bool:
    for key in ('treasury', 'aaa_a', 'bbb', 'bb', 'bonds'):
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
        'treasuries': assets.get('treasury') or [],
        'bonds_aaa_a': assets.get('aaa_a') or [],
        'bonds_bbb': assets.get('bbb') or [],
        'bonds_bb': assets.get('bb') or [],
        'etfs': assets.get('etfs') or [],
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
    """Adapt the ETF report to the common candidate schema.

    ETF selection must use the historical-performance Current Yield produced
    by the market-data ETL, rather than the legacy SPY-only estimate.
    """
    if etfs_df.empty:
        return pd.DataFrame(columns=['Ticker', 'Symbol_x', 'Current Yield_x', 'S&P Equivalent_x'])

    candidates = etfs_df.copy()
    display_symbol = (
        candidates.get('Financial Instrument', candidates.get('Symbol', pd.Series(index=candidates.index)))
        .astype(str)
        .str.strip()
    )
    candidates['Ticker'] = display_symbol
    candidates['Symbol_x'] = display_symbol
    if 'Current Yield' in candidates.columns:
        candidates['Current Yield_x'] = candidates['Current Yield'].apply(
            lambda value: _normalize_yield_percent(_to_float_or_none(value))
        )
    else:
        fallback_yield = _median_equity_yield_percent(
            proposal_equity_df.to_dict(orient='records')
            if proposal_equity_df is not None and not proposal_equity_df.empty
            else []
        )
        logger.warning(
            'ETF snapshot is missing Current Yield; using proposal-equity median fallback '
            f'of {fallback_yield:.4f}% until the ETL snapshot is refreshed.'
        )
        candidates['Current Yield_x'] = fallback_yield
    candidates['S&P Equivalent_x'] = 'ETF'
    # Keep the source columns so optional proposal metadata such as industry
    # can be preserved when it exists in the ETF snapshot.
    return candidates


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
    merged_df = context['merged_df']
    etf_candidates = _prepare_etf_candidates(
        context['etfs_df'],
        context.get('proposal_equity_df'),
    )
    normalized_distribution = _normalize_distribution(distribution)

    used_symbols = _initialize_used_symbols(bonds_df_no_duplicates)
    bucket_selection_profiles: dict[str, dict | None] = {}

    for asset_type in investment_proposal:
        percentage = float(normalized_distribution.get(asset_type['name'], 0) or 0)
        assets_to_invest = int(round(TOTAL_ASSETS * percentage))

        logger.info(f"--- Processing bucket: {asset_type['name']} ---")
        logger.info(f"Distribution percentage: {percentage}")
        logger.info(f"Assets to invest: {assets_to_invest}")
        logger.info(f"Current bonds in bucket: {len(asset_type['bonds'])}")

        sanitized_equivalents = asset_type['equivalents']
        if asset_type['name'] == 'etfs':
            combined_df = etf_candidates
        elif asset_type['name'] == 'treasuries':
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

        ranked_candidates, selection_profile = _prepare_bucket_candidates(
            candidate_df=combined_df,
            used_symbols=used_symbols,
        )
        bucket_selection_profiles[asset_type['name']] = selection_profile
        top_bonds = ranked_candidates.head(max(0, assets_to_invest - len(asset_type['bonds'])))

        normalized_top_bonds = []
        for _, row in top_bonds.iterrows():
            row_data = row.to_dict()
            normalized_top_bonds.append({
                'Symbol_x': row['Symbol_x'],
                'Current Yield_x': row['Current Yield_x'],
                'S&P Equivalent_x': _resolve_rating(row_data) or row['S&P Equivalent_x'],
                **_resolve_asset_metadata(row_data),
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
            _clean_candidate_pool(merged_df[~merged_df['Ticker'].isin(used_symbols)])
        )
        scored_remaining_pool = []
        for _, row in remaining_pool.iterrows():
            rating_key = str(row['S&P Equivalent_x']).replace('+', '').replace('-', '')
            bucket = rating_to_bucket.get(rating_key)
            if not bucket:
                bucket = next(bucket_ref for bucket_ref in investment_proposal if bucket_ref['name'] == 'bonds_bb')

            selection_profile = bucket_selection_profiles.get(bucket['name'])
            target_yield = float(selection_profile['target_yield']) if selection_profile else float(row['Current Yield_x'])
            spread = float(selection_profile['spread']) if selection_profile else 1.0
            selector_score = max(0.0, 1.0 - (abs(float(row['Current Yield_x']) - target_yield) / spread))

            scored_remaining_pool.append({
                **row.to_dict(),
                '_bucket_name': bucket['name'],
                '_selector_score': selector_score,
            })

        remaining_pool = pd.DataFrame(scored_remaining_pool)
        if not remaining_pool.empty:
            remaining_pool = (
                remaining_pool
                    .sort_values(by=['_selector_score', 'Current Yield_x'], ascending=[False, False])
                    .groupby('Ticker')
                    .head(1)
            )

        for _, row in remaining_pool.iterrows():
            if remaining_needed == 0:
                break

            bucket = next(
                bucket_ref for bucket_ref in investment_proposal
                if bucket_ref['name'] == str(row.get('_bucket_name') or 'bonds_bb')
            )

            if bucket_needs[bucket['name']] <= 0:
                continue

            row_data = row.to_dict()
            bucket['bonds'].append({
                'Symbol_x': row['Symbol_x'],
                'Current Yield_x': row['Current Yield_x'],
                'S&P Equivalent_x': row['S&P Equivalent_x'],
                **_resolve_asset_metadata(row_data),
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
    return sum(float(bond.get('current_yield') or 0) for bond in bonds) / len(bonds)


def _build_investment_proposal_preview(
    investment_proposal: list[dict],
    risk_profile_id,
    distribution: dict,
    starting_amount=None,
):
    normalized_distribution = _normalize_distribution(distribution)
    proposal_record = _serialize_investment_proposal(
        investment_proposal=investment_proposal,
        risk_profile_id=risk_profile_id,
        source_type='portfolio_plan',
        starting_amount=starting_amount,
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
    risk_profile_id = proposal.get('risk_profile_id')

    # The proposal's saved assets are authoritative for every source type.
    # Risk archetypes are only a compatibility fallback for legacy rows that
    # have no persisted assets.
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
    if normalized_source_type not in {'risk_profile', 'portfolio_plan', 'custom'}:
        normalized_source_type = 'risk_profile'

    raw_starting_amount = proposal.get('starting_amount')
    try:
        normalized_starting_amount = float(raw_starting_amount) if raw_starting_amount is not None else None
    except (TypeError, ValueError):
        normalized_starting_amount = None

    return {
        **proposal,
        'source_type': normalized_source_type,
        'starting_amount': normalized_starting_amount,
        'assets': _assets_from_saved_proposal(proposal),
        'derived_distribution': _derive_distribution_for_saved_proposal({**proposal, 'source_type': normalized_source_type}),
    }

@handle_exception
def create_investment_proposal_with_assets(assets: list[dict], risk_profile_id=None, contact_id=None, starting_amount=None):
    logger.announcement('Generating investment proposal from assets...')

    try:
        context = _load_investment_proposal_context()
        merged_universe_df = context['merged_universe_df']
        rtd_df = context['rtd_df']
        ust_df = context['ust_df']
        stocks_df = context['stocks_df']
        etfs_df = context['etfs_df']
        investment_proposal = _build_investment_proposal_template()

        if not isinstance(assets, list):
            raise Exception('assets must be a list of dicts with symbol, percentage, and optional source_bucket.')

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
            source_bucket = _resolve_source_bucket(asset)

            if not symbol or percentage is None:
                raise Exception('Each asset must include symbol and percentage.')

            normalized_symbol = str(symbol).strip()
            matched_row = None
            bucket_name = ''
            rating = ''
            current_yield_pct = 0.0

            if source_bucket == 'UST':
                matched_row = _find_matching_market_row(ust_df, normalized_symbol)
                bucket_name = 'treasuries'
                rating = 'UST'
                if matched_row:
                    current_yield_pct = _resolve_current_yield_percent(matched_row)
            elif source_bucket == 'BONDS':
                matched_row = _find_matching_market_row(rtd_df, normalized_symbol)
                if matched_row:
                    rating = _resolve_rating(matched_row)
                    current_yield_pct = _resolve_current_yield_percent(matched_row)
            elif source_bucket == 'ETFS':
                matched_row = _find_matching_market_row(etfs_df, normalized_symbol)
                bucket_name = 'etfs'
                rating = 'ETF'
                if matched_row:
                    current_yield_pct = _resolve_equity_yield_percent(matched_row)
                if current_yield_pct <= 0:
                    current_yield_pct = float(context.get('proposal_equity_yield_pct') or 0)
            elif source_bucket == 'STOCKS':
                matched_row = _find_matching_market_row(stocks_df, normalized_symbol)
                bucket_name = 'stocks'
                rating = 'STOCK'
                if matched_row:
                    current_yield_pct = _resolve_equity_yield_percent(matched_row)
                if current_yield_pct <= 0:
                    current_yield_pct = float(context.get('proposal_equity_yield_pct') or 0)
            else:
                matched_row = _find_matching_market_row(merged_universe_df, normalized_symbol)
                if matched_row:
                    rating = _resolve_rating(matched_row)
                    current_yield_pct = _resolve_current_yield_percent(matched_row)
                else:
                    matched_row = _find_matching_market_row(etfs_df, normalized_symbol)
                    if matched_row:
                        bucket_name = 'etfs'
                        rating = 'ETF'
                        current_yield_pct = _resolve_equity_yield_percent(matched_row)
                    else:
                        matched_row = _find_matching_market_row(stocks_df, normalized_symbol)
                        if matched_row:
                            bucket_name = 'etfs'
                            rating = 'ETF'
                            current_yield_pct = _resolve_equity_yield_percent(matched_row)

            if not matched_row:
                raise Exception(
                    f'Asset "{symbol}" not found in the expected market data feed'
                    f'{f" ({source_bucket})" if source_bucket else ""}.'
                )

            if not rating:
                rating = _resolve_rating(matched_row)

            if not rating:
                raise Exception(
                    f'No rating or asset classification found for "{symbol}". '
                    f'Fields: SP="{matched_row.get("SP", "")}", '
                    f'S&P Equivalent="{matched_row.get("S&P Equivalent", "")}", '
                    f'Ratings="{matched_row.get("Ratings", "")}", '
                    f'Moodys="{matched_row.get("Moodys", "")}".'
                )

            if not bucket_name:
                bucket_name = _get_bucket_for_rating(rating)
            if not bucket_name and source_bucket in {'STOCKS', 'ETFS'}:
                bucket_name = 'stocks' if source_bucket == 'STOCKS' else 'etfs'
                rating = 'STOCK' if source_bucket == 'STOCKS' else 'ETF'
            if not bucket_name:
                raise Exception(f'Unknown rating "{rating}" for asset {symbol}.')

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
    except Exception as exc:
        logger.error(f'Failed creating investment proposal: {exc}')
        raise Exception(f'Failed creating investment proposal: {exc}')

    return _persist_investment_proposal(investment_proposal, risk_profile_id, 'custom', contact_id=contact_id, starting_amount=starting_amount)


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
        )

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
        )

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
        )
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
def read_investment_proposals(query: dict = None):
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
