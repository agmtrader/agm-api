from src.utils.position_quotes import enrich_position_quotes


def snapshot(rows, **kwargs):
    return {'name': 'ibkr_stocks_snapshot.csv', 'asset_class': 'STK', 'modified_time': '2026-10-12T10:00:00Z', 'rows': rows, **kwargs}


def test_contract_match_preserves_positions_and_values():
    position = {'Conid': '123.0', 'Symbol': 'OTHER', 'AssetClass': 'STK', 'CurrencyPrimary': 'USD', 'Quantity': 8, 'MarkPrice': 12}
    source = snapshot([{'Conid': 123, 'Symbol': 'ABC', 'Trading Currency': 'USD', 'Bid': '11.2', 'Ask': '11.4', 'Timestamp': '2026-10-12T09:00:00Z'}])
    row = enrich_position_quotes([position], [source])[0]
    assert row['Bid'] == 11.2 and row['Ask'] == 11.4
    assert row['MarkPrice'] == 12 and row['Quantity'] == 8 and row['QuoteStatus'] == 'matched'
    assert row['QuoteTimestamp'] != row['QuoteSourceModifiedAt']
    assert 'Bid' not in position


def test_symbol_fallback_requires_class_currency_and_uniqueness():
    position = {'Symbol': 'US123', 'AssetClass': 'BOND', 'CurrencyPrimary': 'USD'}
    row = {'Symbol': 'US123', 'Trading Currency': 'USD', 'Bid': 99, 'Ask': 100}
    source = snapshot([row], asset_class='BOND')
    assert enrich_position_quotes([position], [source])[0]['Bid'] == 99
    assert enrich_position_quotes([{**position, 'CurrencyPrimary': 'EUR'}], [source])[0]['Bid'] is None
    assert enrich_position_quotes([{**position, 'AssetClass': 'OPT'}], [source])[0]['Bid'] is None
    result = enrich_position_quotes([position], [source, source])[0]
    assert result['Bid'] is None and result['QuoteStatus'] == 'ambiguous'


def test_missing_invalid_and_unmatched_quotes_are_not_zero_or_last_prices():
    position = {'Conid': 123, 'Symbol': 'ABC', 'AssetClass': 'STK', 'CurrencyPrimary': 'USD'}
    for bad in [None, '', 0, -1, 'NaN', 'inf', 'unavailable']:
        source = snapshot([{'Conid': 123, 'Bid': bad, 'Ask': bad, 'Last': 25}])
        row = enrich_position_quotes([position], [source])[0]
        assert row['Bid'] is None and row['Ask'] is None and row['QuoteStatus'] == 'unavailable'
    assert enrich_position_quotes([position], [snapshot([])])[0]['Bid'] is None


def test_ambiguous_contract_does_not_fall_back_to_a_different_symbol():
    position = {'Conid': 123, 'Symbol': 'ABC', 'AssetClass': 'STK', 'CurrencyPrimary': 'USD'}
    source = snapshot([{'Conid': 123, 'Symbol': 'XXX', 'Bid': 9}, {'Conid': 123, 'Symbol': 'YYY', 'Bid': 10}, {'Conid': 999, 'Symbol': 'ABC', 'Trading Currency': 'USD', 'Bid': 20}])
    row = enrich_position_quotes([position], [source])[0]
    assert row['Bid'] is None and row['QuoteStatus'] == 'ambiguous'


def test_real_reporting_quote_reader_uses_snapshot_cache(monkeypatch):
    from src.components.tools.public import reporting
    from src.utils.report_cache import ReportCache
    from unittest.mock import Mock
    monkeypatch.setattr(reporting, '_quote_snapshot_cache', ReportCache(30, keys=('market-quotes',)))
    loader = Mock(return_value=[snapshot([{'Conid': 123, 'Bid': 10, 'Ask': 11}])])
    monkeypatch.setattr(reporting, '_read_quote_snapshots', loader)
    position = {'Conid': 123, 'CurrencyPrimary': 'USD'}
    assert reporting.add_open_position_quotes([position])[0]['Bid'] == 10
    assert reporting.add_open_position_quotes([position])[0]['Ask'] == 11
    assert loader.call_count == 1


def test_bond_snapshot_ibcid_symbol_matches_exact_position_contract():
    position = {'Conid': 44260428, 'Symbol': 'GS 6.45 05/01/36', 'AssetClass': 'BOND', 'CurrencyPrimary': 'USD'}
    source = snapshot([{'Symbol': 'IBCID44260428', 'Trading Currency': 'USD', 'Bid': 102.3, 'Ask': 102.7}], asset_class='BOND')
    assert enrich_position_quotes([position], [source])[0]['Bid'] == 102.3
    assert enrich_position_quotes([{**position, 'Conid': 999}], [source])[0]['Bid'] is None


def test_optional_reporting_filter_retains_only_requested_accounts(monkeypatch):
    from src.components.tools.public import reporting
    from unittest.mock import Mock
    drive=Mock()
    drive.get_files_in_folder.return_value=[{'id':'positions-file','name':'ibkr_open_positions_all.csv'}]
    drive.download_file.return_value=[{'ClientAccountID':'U-OWN','Conid':123},{'ClientAccountID':'U-OTHER','Conid':456}]
    monkeypatch.setattr(reporting,'Drive',drive)
    enriched=Mock(side_effect=lambda rows:[{**row,'Bid':10} for row in rows])
    monkeypatch.setattr(reporting,'add_open_position_quotes',enriched)
    assert reporting.get_open_positions_report(True,['U-OWN'])==[{'ClientAccountID':'U-OWN','Conid':123,'Bid':10}]
    assert len(reporting.get_open_positions_report())==2
    assert enriched.call_args.args[0]==[{'ClientAccountID':'U-OWN','Conid':123}]
