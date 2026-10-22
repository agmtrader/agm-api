"""Direct component and Flask route regressions; all external services are mocked.

Run: venv/bin/python -m unittest discover -s tests -p test_investment_proposals.py -v
"""
import importlib.util
from pathlib import Path
import sys
import types
import unittest
import pytest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_source(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_components():
    logger_module = types.ModuleType('src.utils.logger')
    logger_module.logger = MagicMock()
    db_module = types.ModuleType('src.utils.connectors.supabase')
    db_module.db = MagicMock()
    db_module.db.read.return_value = []
    reporting = types.ModuleType('src.components.tools.public.reporting')
    for name in ['get_bond_report', 'get_etfs_report', 'get_open_positions_report',
                 'get_proposals_equity_report', 'get_stocks_report', 'get_ust_bond_report']:
        setattr(reporting, name, MagicMock(return_value=[]))
    with patch.dict(sys.modules, {
        'src.utils.logger': logger_module,
        'src.utils.connectors.supabase': db_module,
        'src.components.tools.public.reporting': reporting,
    }):
        errors = load_source('src.utils.exception', 'src/utils/exception.py')
        with patch.dict(sys.modules, {'src.utils.exception': errors}):
            risk = load_source('src.components.clients.risk_profiles', 'src/components/clients/risk_profiles.py')
            response = load_source('src.utils.response', 'src/utils/response.py')
            with patch.dict(sys.modules, {
                'src.components.clients.risk_profiles': risk,
                'src.utils.response': response,
            }):
                proposals = load_source('src.components.clients.investment_proposals', 'src/components/clients/investment_proposals.py')
                with patch.dict(sys.modules, {'src.components.clients.investment_proposals': proposals}):
                    routes = load_source('src.app.clients.investment_proposals', 'src/app/clients/investment_proposals.py')
    return proposals, errors, routes


class InvestmentProposalTests(unittest.TestCase):
    def setUp(self):
        self.p, self.errors, self.routes = load_components()
        self.fallback = pd.DataFrame([{'Current Yield': 8.0}])
        self.etfs = pd.DataFrame([
            {'Financial Instrument': symbol, 'Current Yield': None}
            for symbol in ['SPY', 'QQQ', 'IWM', 'VTI']
        ])
        self.bonds = pd.DataFrame([
            {'Ticker': f'TEST{rating}{i}', 'Symbol_x': f'TEST{rating}{i}',
             'S&P Equivalent_x': rating, 'Current Yield_x': 6.0}
            for rating in ['BBB', 'BB'] for i in range(24)
        ])
        self.context = {
            'etfs_df': pd.DataFrame([{'Financial Instrument': f'ETF{i}', 'Current Yield': 8.0} for i in range(20)]),
            'proposal_equity_df': self.fallback,
            'bonds_df_no_duplicates': pd.DataFrame(),
            'merged_df': self.bonds,
        }

    def populate(self, score=3.2):
        proposal = self.p._build_investment_proposal_template()
        archetype = self.p.get_risk_archetype_for_score(score)
        distribution = self.p._distribution_from_risk_archetype(archetype)
        self.p._populate_investment_proposal_from_distribution(proposal, distribution, self.context)
        return proposal, distribution

    def test_missing_column_uses_existing_estimate(self):
        candidates = self.p._prepare_etf_candidates(self.etfs.drop(columns=['Current Yield']), self.fallback)
        self.assertEqual(candidates['Current Yield_x'].tolist(), [8.0] * 4)

    def test_empty_null_invalid_and_nonfinite_values_use_estimate(self):
        for value in [None, '', 'n/a', float('nan'), float('inf'), float('-inf')]:
            with self.subTest(value=value):
                candidates = self.p._prepare_etf_candidates(self.etfs.assign(**{'Current Yield': value}), self.fallback)
                self.assertEqual(candidates['Current Yield_x'].tolist(), [8.0] * 4)

    def test_mixed_values_preserve_valid_yields(self):
        snapshot = self.etfs.assign(**{'Current Yield': [None, '12.5%', 0.09, 0]})
        candidates = self.p._prepare_etf_candidates(snapshot, self.fallback)
        self.assertEqual(candidates['Current Yield_x'].tolist(), [8.0, 12.5, 9.0, 0.0])

    def test_real_zero_negative_and_outlier_yields_are_not_replaced(self):
        snapshot = self.etfs.assign(**{'Current Yield': [0, -2, 30, 9]})
        candidates = self.p._prepare_etf_candidates(snapshot, self.fallback)
        self.assertEqual(candidates['Current Yield_x'].tolist(), [0.0, -2.0, 30.0, 9.0])
        self.assertEqual(self.p._clean_candidate_pool(candidates)['Ticker'].tolist(), ['VTI'])

    def test_aggressive_c_populates_etfs_with_estimate(self):
        proposal, _ = self.populate()
        assets = self.p._assets_from_investment_proposal(proposal)
        self.assertEqual(len(assets['etfs']), 20)
        self.assertFalse(assets['bonds'])
        self.assertAlmostEqual(sum(asset['percentage'] for asset in assets['etfs']), 1.0)

    def test_mixed_archetypes_preserve_allocations_after_serialization(self):
        for score in [2.1, 2.45, 2.85]:
            with self.subTest(score=score):
                proposal, expected = self.populate(score)
                record = self.p._serialize_investment_proposal(proposal, None, 'risk_profile')
                normalized = self.p._normalize_saved_investment_proposal(record)
                self.assertEqual(sum(len(normalized['assets'][key]) for key in ['treasury','aaa_a','bbb','bb','etfs']), 20)
                for bucket, weight in expected.items():
                    self.assertAlmostEqual(normalized['derived_distribution'][bucket], weight)
                self.p.db.create.assert_not_called()

    def test_missing_etfs_fails_before_persistence(self):
        for snapshot, fallback in [(pd.DataFrame(), self.fallback), (self.etfs, pd.DataFrame())]:
            with self.subTest(empty_snapshot=snapshot.empty):
                self.context.update(etfs_df=snapshot, proposal_equity_df=fallback)
                with patch.object(self.p, '_load_investment_proposal_context', return_value=self.context):
                    with self.assertRaises(self.errors.ServiceError) as caught:
                        self.p.create_investment_proposal_with_risk_profile({'score': 3.2, 'id': 'test'})
                self.assertEqual(caught.exception.status_code, 422)
                self.assertEqual(caught.exception.code, 'proposal_assets_unavailable')
                self.assertEqual(caught.exception.details['missing_buckets'], ['etfs'])
                self.p.db.create.assert_not_called()

    def test_missing_bond_sleeve_does_not_silently_reallocate(self):
        self.context['merged_df'] = self.bonds[self.bonds['S&P Equivalent_x'] == 'BBB']
        with self.assertRaises(self.errors.ServiceError) as caught:
            self.populate(2.45)
        self.assertIn('bonds_bb', caught.exception.details['missing_buckets'])

    def test_unsupported_ratings_never_fill_a_bb_allocation(self):
        for rating in ['B', 'CCC', '', 'UNRATED']:
            with self.subTest(rating=rating):
                unsupported = pd.DataFrame([
                    {'Ticker': f'LOW{i}', 'Symbol_x': f'LOW{i}',
                     'S&P Equivalent_x': rating, 'Current Yield_x': 6.0}
                    for i in range(4)
                ])
                self.context['merged_df'] = pd.concat([
                    self.bonds[self.bonds['S&P Equivalent_x'] == 'BBB'], unsupported,
                ], ignore_index=True)
                with self.assertRaises(self.errors.ServiceError) as caught:
                    self.populate(2.85)
                self.assertEqual(caught.exception.details['missing_buckets'], ['bonds_bb'])

    def test_rating_fields_normalize_without_substring_false_positives(self):
        cases = [
            ({'S&P Equivalent': ' bb− '}, 'BB-'),
            ({'S&P Equivalent_x': 'bb+'}, 'BB+'),
            ({'S&P Equivalent': float('nan'), 'SP': 'BB'}, 'BB'),
            ({'S&P Equivalent': 'UNRATED', 'Moodys': 'Ba2'}, 'BB'),
            ({'Moodys': 'Ba1'}, 'BB+'), ({'Moodys': 'Ba2'}, 'BB'),
            ({'Moodys': 'Ba3'}, 'BB-'),
            ({'Ratings': 'Ba2'}, 'BB'), ({'Ratings': 'Baa2'}, 'BBB'),
            ({'Ratings': 'BB-'}, 'BB-'),
            ({'Ratings': 'CAA3/CCC+ (MOODY/SP)'}, 'CCC-'),
            ({'Ratings': 'S&P: BB / Moody’s: Ba2'}, 'BB'),
            ({'SP': 'BB', 'Moodys': 'B2'}, 'B'),
            ({'S&P Equivalent': 'CCC', 'SP': 'BB'}, 'CCC'),
            ({'S&P Equivalent_x': 'BB', 'Ratings': 'UNRATED'}, 'BB'),
        ]
        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(self.p._resolve_rating(row), expected)
        for value in ['UNRATED', 'Not Rated', 'NR', 'Moody', 'N/A', None, float('nan'), 'BBQ', 'AAAfoo']:
            with self.subTest(unrated=value):
                self.assertEqual(self.p._resolve_rating({'Ratings': value}), '')

    def test_bucket_lookup_preserves_asset_classes_and_rejects_invalid_bond_ratings(self):
        for rating, expected in [('UST', 'treasuries'), ('ETF', 'etfs'), ('STOCK', 'stocks'),
                                 (' bb− ', 'bonds_bb'), ('BBB+', 'bonds_bbb'), ('A-', 'bonds_aaa_a')]:
            with self.subTest(rating=rating):
                self.assertEqual(self.p._get_bucket_for_rating(rating), expected)
        for rating in ['B', 'CCC', 'UNRATED', 'BB--', 'AAA+', None, float('nan')]:
            with self.subTest(invalid=rating):
                self.assertEqual(self.p._get_bucket_for_rating(rating), '')

    def test_normalized_bb_equivalents_survive_generation_and_serialization(self):
        rows = []
        inputs = [
            {'S&P Equivalent_x': ' bb '}, {'S&P Equivalent_x': 'BB+'},
            {'S&P Equivalent_x': 'bb−'}, {'Moodys': 'Ba1'},
            {'Moodys': 'Ba2'}, {'Moodys': 'Ba3'}, {'Ratings': 'Ba2'},
            {'S&P Equivalent_x': float('nan'), 'SP': 'BB-'},
        ]
        for i, fields in enumerate(inputs):
            rows.append({'Ticker': f'VALID{i}', 'Symbol_x': f'VALID{i}', 'Current Yield_x': 6.0, **fields})
        self.context['merged_df'] = pd.DataFrame(rows)
        proposal = self.p._build_investment_proposal_template()
        self.p._populate_investment_proposal_from_distribution(proposal, {'bonds_bb': 1}, self.context, total_assets=len(rows))
        record = self.p._normalize_saved_investment_proposal(self.p._serialize_investment_proposal(proposal, None, 'risk_profile'))
        assets = record['assets']['bb']
        self.assertEqual({a['symbol'] for a in assets}, {f'VALID{i}' for i in range(len(inputs))})
        self.assertEqual({a['equivalent'] for a in assets}, {'BB', 'BB+', 'BB-'})
        self.assertAlmostEqual(sum(a['percentage'] for a in assets), 1)

    def test_ineligible_bonds_are_excluded_even_when_bb_assets_exist(self):
        invalid = ['B', 'B+', 'CCC', 'CC', 'C', 'D', 'SD', '', 'UNRATED', 'Not Rated']
        rows = [{'Ticker': f'INVALID{i}', 'Symbol_x': f'INVALID{i}', 'S&P Equivalent_x': rating,
                 'Ratings': rating, 'Current Yield_x': 6.0} for i, rating in enumerate(invalid)]
        rows.append({'Ticker': 'VALID', 'Symbol_x': 'VALID', 'S&P Equivalent_x': 'BB-', 'Current Yield_x': 6.0})
        self.context['merged_df'] = pd.DataFrame(rows)
        proposal = self.p._build_investment_proposal_template()
        self.p._populate_investment_proposal_from_distribution(proposal, {'bonds_bb': 1}, self.context, total_assets=1)
        assets = self.p._assets_from_investment_proposal(proposal)['bb']
        self.assertEqual([a['symbol'] for a in assets], ['VALID'])
        self.assertEqual(assets[0]['percentage'], 1)

    def test_all_archetypes_preserve_exact_weights_and_asset_ratings(self):
        self.context['merged_df'] = pd.DataFrame([
            {'Ticker': f'{rating}{i}', 'Symbol_x': f'{rating}{i}',
             'S&P Equivalent_x': rating, 'Current Yield_x': 6.0}
            for rating in ['AAA', 'BBB', 'BB', 'UST'] for i in range(30)
        ])
        self.context['etfs_df'] = pd.DataFrame([
            {'Financial Instrument': f'ETF{i}', 'Current Yield': None} for i in range(30)
        ])
        mapping = {
            'treasuries': ('treasury', {'UST'}),
            'bonds_aaa_a': ('aaa_a', {'AAA', 'AA', 'A'}),
            'bonds_bbb': ('bbb', {'BBB'}),
            'bonds_bb': ('bb', {'BB'}),
            'etfs': ('etfs', {'ETF'}),
        }
        for archetype in self.p.risk_archetypes:
            with self.subTest(archetype=archetype['name']):
                expected = self.p._distribution_from_risk_archetype(archetype)
                self.assertAlmostEqual(sum(expected.values()), 1.0)
                proposal = self.p._build_investment_proposal_template()
                self.p._populate_investment_proposal_from_distribution(proposal, expected, self.context)
                record = self.p._normalize_saved_investment_proposal(
                    self.p._serialize_investment_proposal(proposal, None, 'risk_profile')
                )
                for key, weight in expected.items():
                    bucket, ratings = mapping[key]
                    assets = record['assets'][bucket]
                    self.assertAlmostEqual(record['derived_distribution'][key], weight)
                    self.assertEqual(bool(assets), weight > 0)
                    self.assertAlmostEqual(sum(asset['percentage'] for asset in assets), weight)
                    self.assertTrue(all(asset['equivalent'] in ratings for asset in assets))
                defaults = {
                    'selected_risk_archetype': archetype['name'],
                    'allocation': self.p._default_allocation_from_risk_archetype(archetype),
                    'bond_rating_allocation': self.p._default_bond_rating_allocation_from_risk_archetype(archetype),
                }
                self.assertEqual(self.p._distribution_from_portfolio_plan(defaults), expected)

    def test_small_positive_sleeve_gets_at_least_one_asset(self):
        proposal = self.p._build_investment_proposal_template()
        self.p._populate_investment_proposal_from_distribution(proposal, {'bonds_bbb': 0.99, 'etfs': 0.01}, self.context)
        etfs = next(bucket['bonds'] for bucket in proposal if bucket['name'] == 'etfs')
        self.assertGreaterEqual(len(etfs), 1)
        self.assertAlmostEqual(sum(asset['percentage'] for asset in etfs), 0.01)

    def test_bond_only_proposal_does_not_require_etf_yields(self):
        self.context.update(etfs_df=pd.DataFrame(), proposal_equity_df=pd.DataFrame())
        proposal = self.p._build_investment_proposal_template()
        self.p._populate_investment_proposal_from_distribution(proposal, {'bonds_bbb': 1}, self.context)
        self.assertEqual(sum(len(bucket['bonds']) for bucket in proposal), 20)

    def test_all_zero_distribution_fails(self):
        with self.assertRaises(self.errors.ServiceError):
            self.p._populate_investment_proposal_from_distribution(self.p._build_investment_proposal_template(), {}, self.context)

    def test_successful_risk_route_returns_etfs(self):
        app = Flask(__name__)
        app.register_blueprint(self.routes.bp, url_prefix='/investment_proposals')
        def persist(proposal, risk_profile_id, source_type, **kwargs):
            return self.p._serialize_investment_proposal(proposal, risk_profile_id, source_type, **kwargs)
        with patch.object(self.p, '_load_investment_proposal_context', return_value=self.context), \
             patch.object(self.p, '_persist_investment_proposal', side_effect=persist):
            response = app.test_client().post('/investment_proposals/create/risk', json={'risk_profile': {'score': 3.2, 'id': 'test'}})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json['assets']['etfs']), 20)

    def test_missing_eligible_bb_returns_422_without_persistence_on_all_routes(self):
        app = Flask(__name__)
        app.register_blueprint(self.routes.bp, url_prefix='/investment_proposals')
        plan = {'allocation': {'bonds': 20, 'stocks': 80},
                'bond_rating_allocation': {'bbb': 20, 'bb': 80}}
        cases = [
            ('create/risk', {'risk_profile': {'score': 2.85, 'id': 'test'}}),
            ('create/plan', {'portfolio_plan': plan}),
            ('preview/plan', {'portfolio_plan': plan}),
        ]
        for rating in ['B', 'CCC', '', 'UNRATED']:
            invalid = pd.DataFrame([
                {'Ticker': f'INVALID{i}', 'Symbol_x': f'INVALID{i}',
                 'S&P Equivalent_x': rating, 'Current Yield_x': 6.0} for i in range(4)
            ])
            self.context['merged_df'] = pd.concat([
                self.bonds[self.bonds['S&P Equivalent_x'] == 'BBB'], invalid,
            ], ignore_index=True)
            with patch.object(self.p, '_load_investment_proposal_context', return_value=self.context), \
                 patch.object(self.p, '_persist_investment_proposal') as persist, \
                 patch.object(self.p, '_build_investment_proposal_preview') as preview:
                for route, payload in cases:
                    with self.subTest(rating=rating, route=route):
                        response = app.test_client().post('/investment_proposals/' + route, json=payload)
                        self.assertEqual(response.status_code, 422)
                        self.assertEqual(response.json['code'], 'proposal_assets_unavailable')
                        self.assertEqual(response.json['details']['missing_buckets'], ['bonds_bb'])
                        self.assertIn('no eligible assets for bonds_bb', response.json['error'])
                        persist.assert_not_called()
                        preview.assert_not_called()
                        self.p.db.create.assert_not_called()
                        self.p.db.update.assert_not_called()

    def test_plan_preview_keeps_valid_bb_and_excludes_lower_ratings(self):
        app = Flask(__name__)
        app.register_blueprint(self.routes.bp, url_prefix='/investment_proposals')
        self.context['merged_df'] = pd.DataFrame([
            {'Ticker': 'VALID', 'Symbol_x': 'VALID', 'Moodys': 'Ba3', 'Current Yield_x': 6.0},
            {'Ticker': 'INVALID', 'Symbol_x': 'INVALID', 'S&P Equivalent_x': 'B', 'Current Yield_x': 6.0},
        ])
        plan = {'total_assets': 1, 'allocation': {'bonds': 100}, 'bond_rating_allocation': {'bb': 100}}
        with patch.object(self.p, '_load_investment_proposal_context', return_value=self.context), \
             patch.object(self.p, '_persist_investment_proposal') as persist:
            response = app.test_client().post('/investment_proposals/preview/plan', json={'portfolio_plan': plan})
        self.assertEqual(response.status_code, 200)
        assets = response.json['assets']['bb']
        self.assertEqual([a['symbol'] for a in assets], ['VALID'])
        self.assertEqual(assets[0]['equivalent'], 'BB-')
        self.assertEqual(assets[0]['percentage'], 1)
        persist.assert_not_called()
        self.p.db.create.assert_not_called()
        self.p.db.update.assert_not_called()

    def test_risk_plan_and_preview_routes_return_useful_422(self):
        self.context['etfs_df'] = pd.DataFrame()
        app = Flask(__name__)
        app.register_blueprint(self.routes.bp, url_prefix='/investment_proposals')
        cases = [
            ('create/risk', {'risk_profile': {'score': 3.2, 'id': 'test'}}),
            ('create/plan', {'portfolio_plan': {'allocation': {'stocks': 100}}}),
            ('preview/plan', {'portfolio_plan': {'allocation': {'stocks': 100}}}),
        ]
        with patch.object(self.p, '_load_investment_proposal_context', return_value=self.context):
            for route, payload in cases:
                with self.subTest(route=route):
                    response = app.test_client().post('/investment_proposals/' + route, json=payload)
                    self.assertEqual(response.status_code, 422)
                    self.assertEqual(response.json['code'], 'proposal_assets_unavailable')
                    self.assertIn('no eligible assets for etfs', response.json['error'])
                    self.p.db.create.assert_not_called()


if __name__ == '__main__':
    unittest.main()


def test_total_count_optimizer_matches_exhaustive_weighted_return():
    p, errors, _ = load_components()
    context = {
        'bonds_df_no_duplicates': pd.DataFrame(),
        'merged_df': pd.DataFrame([{'Ticker': f'B{i}', 'Symbol_x': f'B{i}', 'S&P Equivalent_x': 'BBB', 'Current Yield_x': y} for i, y in enumerate([6, 5.9, 5.8, 5.7])]),
        'etfs_df': pd.DataFrame([{'Financial Instrument': f'E{i}', 'Current Yield': y} for i, y in enumerate([10, 9.9, 9.8, 9.7])]),
    }
    proposal = p._build_investment_proposal_template()
    p._populate_investment_proposal_from_distribution(proposal, {'etfs': .6, 'bonds_bbb': .4}, context, total_assets=5)
    record = p._build_investment_proposal_preview(proposal, None, {'etfs': .6, 'bonds_bbb': .4})
    # Independent enumeration of every feasible split; counts do not follow 60/40.
    best = max(.6 * np.mean([10,9.9,9.8,9.7][:e]) + .4 * np.mean([6,5.9,5.8,5.7][:5-e]) for e in range(1,5))
    assert record['total_assets'] == 5
    assert abs(record['expected_average_yield'] - best) < 1e-6
    assert sum(a['percentage'] for a in record['assets']['etfs']) == .6
    assert record['assets']['treasury'] == []
    assert all(bucket['weight'] > 0 for bucket in record['bucket_summaries'])
    for invalid in [0, -1, 1.5, True, '5']:
        with pytest.raises(errors.ServiceError):
            p._populate_investment_proposal_from_distribution(p._build_investment_proposal_template(), {'etfs':1}, context, total_assets=invalid)
    with pytest.raises(errors.ServiceError) as caught:
        p._populate_investment_proposal_from_distribution(p._build_investment_proposal_template(), {'etfs':1}, context, total_assets=20)
    assert caught.value.code == 'proposal_asset_count_unavailable'


def test_plan_preview_and_save_share_requested_count_and_weighted_output():
    p, _, routes = load_components()
    app = Flask(__name__)
    app.register_blueprint(routes.bp, url_prefix='/investment_proposals')
    context = {
        'bonds_df_no_duplicates': pd.DataFrame(),
        'merged_df': pd.DataFrame([{'Ticker':f'BBB{i}', 'Symbol_x':f'BBB{i}', 'S&P Equivalent_x':'BBB', 'Current Yield_x':5.0} for i in range(30)]),
        'etfs_df': pd.DataFrame([{'Financial Instrument':f'ETF{i}', 'Current Yield':8.0} for i in range(30)]),
        'proposal_equity_df': pd.DataFrame(),
    }
    plan = {'total_assets': 13, 'starting_amount': 12345.67, 'allocation':{'bonds':40,'stocks':60}, 'bond_rating_allocation':{'bbb':100}}
    def persist(portfolio, risk_id, source, **kwargs):
        return p._normalize_saved_investment_proposal(p._serialize_investment_proposal(portfolio, risk_id, source, **kwargs))
    with patch.object(p, '_load_investment_proposal_context', return_value=context), patch.object(p, '_persist_investment_proposal', side_effect=persist):
        preview = app.test_client().post('/investment_proposals/preview/plan', json={'portfolio_plan':plan})
        saved = app.test_client().post('/investment_proposals/create/plan', json={'portfolio_plan':plan})
    assert preview.status_code == saved.status_code == 200
    a,b = preview.get_json(),saved.get_json()
    assert a['total_assets'] == 13
    assert a['assets'] == b['assets']
    assert a['starting_amount'] == b['starting_amount'] == 12345.67
    assert abs(a['expected_average_yield'] - (5*.4+8*.6)) < 1e-6
    assert abs(sum(asset['percentage']*asset['current_yield'] for key in ['bbb','etfs'] for asset in b['assets'][key]) - a['expected_average_yield']) < 1e-6


def test_custom_preview_uses_selected_weights_and_matches_save_without_writing():
    p, _, routes = load_components()
    app = Flask(__name__)
    app.register_blueprint(routes.bp, url_prefix='/investment_proposals')
    context = {
        'merged_universe_df': pd.DataFrame(), 'ust_df': pd.DataFrame(), 'stocks_df': pd.DataFrame(),
        'rtd_df': pd.DataFrame([{'Symbol':'BOND','SP':'BBB','Current Yield':5}]),
        'etfs_df': pd.DataFrame([{'Symbol':'ETF1','Current Yield':10}, {'Symbol':'ETF2','Current Yield':2}]),
    }
    payload = {'assets':[
        {'symbol':'BOND','source_bucket':'BONDS','percentage':40},
        {'symbol':'ETF1','source_bucket':'ETFS','percentage':50},
        {'symbol':'ETF2','source_bucket':'ETFS','percentage':10},
        {'symbol':'not-in-feed','source_bucket':'UST','percentage':0},
    ]}
    def persist(portfolio, risk_id, source, **kwargs):
        return p._normalize_saved_investment_proposal(p._serialize_investment_proposal(portfolio, risk_id, source, **kwargs))
    with patch.object(p, '_load_investment_proposal_context', return_value=context), patch.object(p, '_persist_investment_proposal', side_effect=persist) as write:
        preview = app.test_client().post('/investment_proposals/preview/assets', json=payload)
        assert preview.status_code == 200
        write.assert_not_called()
        p.db.create.assert_not_called()
        saved = app.test_client().post('/investment_proposals/create/assets', json=payload)
    assert saved.status_code == 200
    a, b = preview.get_json(), saved.get_json()
    assert a['assets'] == b['assets']
    assert a['source_type'] == b['source_type'] == 'custom'
    assert a['total_assets'] == 3
    assert a['assets']['treasury'] == []
    assert a['expected_average_yield'] == 7.2  # .4*5 + .5*10 + .1*2
    assert a['derived_distribution']['etfs'] == pytest.approx(.6)
    assert a['bucket_summaries'][1]['average_yield'] == pytest.approx(5.2/.6)


def test_optimizer_does_not_reserve_unselected_issuers_or_depend_on_bucket_order():
    from itertools import combinations, product
    p, _, _ = load_components()
    pools = [
        [{'Ticker':t, 'Current Yield_x':y} for t,y in [('shared',10),('A',9),('B',8)]],
        [{'Ticker':t, 'Current Yield_x':y} for t,y in [('shared',9),('C',5),('D',4)]],
    ]
    weights = [.2,.8]
    best = -1
    for first,second in product(range(1,4), repeat=2):
        if first+second != 3: continue
        for a,b in product(combinations(pools[0],first),combinations(pools[1],second)):
            if len({row['Ticker'] for row in (*a,*b)}) != 3: continue
            best = max(best, .2*np.mean([r['Current Yield_x'] for r in a])+.8*np.mean([r['Current Yield_x'] for r in b]))
    for rows, shares in [(pools,weights),(pools[::-1],weights[::-1])]:
        selected = p._optimize_bucket_asset_selection(rows,shares,3)
        assert len({r['Ticker'] for sleeve in selected for r in sleeve}) == 3
        actual = sum(w*np.mean([r['Current Yield_x'] for r in sleeve]) for w,sleeve in zip(shares,selected))
        assert actual == pytest.approx(best)
