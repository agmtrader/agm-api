"""Direct component and Flask route regressions; all external services are mocked.

Run: venv/bin/python -m unittest discover -s tests -p test_investment_proposals.py -v
"""
import importlib.util
from pathlib import Path
import sys
import types
import unittest
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
            for rating in ['BBB', 'BB'] for i in range(12)
        ])
        self.context = {
            'etfs_df': self.etfs,
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
        self.assertEqual(len(assets['etfs']), 4)
        self.assertFalse(assets['bonds'])
        self.assertAlmostEqual(sum(asset['percentage'] for asset in assets['etfs']), 1.0)

    def test_mixed_archetypes_preserve_allocations_after_serialization(self):
        for score in [2.1, 2.45, 2.85]:
            with self.subTest(score=score):
                proposal, expected = self.populate(score)
                record = self.p._serialize_investment_proposal(proposal, None, 'risk_profile')
                normalized = self.p._normalize_saved_investment_proposal(record)
                self.assertEqual(len(normalized['assets']['etfs']), 4)
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
        self.assertEqual(len(etfs), 1)
        self.assertAlmostEqual(etfs[0]['percentage'], 0.01)

    def test_bond_only_proposal_does_not_require_etf_yields(self):
        self.context.update(etfs_df=pd.DataFrame(), proposal_equity_df=pd.DataFrame())
        proposal = self.p._build_investment_proposal_template()
        self.p._populate_investment_proposal_from_distribution(proposal, {'bonds_bbb': 1}, self.context)
        self.assertEqual(sum(len(bucket['bonds']) for bucket in proposal), 12)

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
        self.assertEqual(len(response.json['assets']['etfs']), 4)

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
