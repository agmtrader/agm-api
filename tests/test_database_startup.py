from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
import subprocess
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, Text, create_engine, event
from sqlalchemy.orm import declarative_base

from src.utils.managers.database_manager import DatabaseManager
from src.utils.lazy_client import LazyClient


@pytest.fixture
def database():
    base = declarative_base()
    class Item(base):
        __tablename__ = 'item'
        id = Column(Text, primary_key=True)
        created = Column(Text, nullable=False)
        updated = Column(Text, nullable=False)
        name = Column(Text, nullable=True, default='python-default')
    engine = create_engine('sqlite://')
    base.metadata.create_all(engine)  # Synthetic in-memory fixture only.
    statements = []
    event.listen(engine, 'before_cursor_execute', lambda connection, cursor, statement, parameters, context, many: statements.append(statement))
    manager = DatabaseManager(base, engine)
    yield manager, Item, statements
    engine.dispose()


def test_initialization_has_no_database_queries_and_core_defaults_are_isolated(database):
    manager, model, statements = database
    assert statements == []
    assert manager.metadata.tables['item'].c.name.default is None
    assert model.__table__.c.name.default is not None


def test_declared_metadata_preserves_crud_and_bounded_reads_without_reflection(database):
    manager, _, statements = database
    manager.create('item', {'id': 'one', 'name': 'first'})
    manager.create('item', {'id': 'two', 'name': 'second'})
    assert manager.read('item', {'id': 'one'})[0]['name'] == 'first'
    assert len(manager.read('item', {}, limit=1, order_by=('id',))) == 1
    manager.update('item', {'id': 'one'}, {'name': 'changed'})
    assert manager.read('item', {'id': 'one'}, exclude_columns=['created'])[0]['name'] == 'changed'
    assert not any('PRAGMA' in statement or 'sqlite_master' in statement for statement in statements)


def inspector_for_item():
    inspector = Mock()
    inspector.get_table_names.return_value = ['item']
    inspector.get_columns.return_value = [
        {'name': name, 'nullable': name == 'name', 'type': Text()}
        for name in ['id', 'created', 'updated', 'name']
    ]
    inspector.get_pk_constraint.return_value = {'constrained_columns': ['id']}
    inspector.get_foreign_keys.return_value = []
    inspector.get_unique_constraints.return_value = []
    return inspector


@pytest.mark.parametrize('mismatch', ['table', 'column', 'nullable', 'primary_key', 'foreign_key', 'unique', 'type'])
def test_preflight_fails_for_schema_mismatch_even_in_dev_mode(database, monkeypatch, mismatch):
    manager, _, _ = database
    monkeypatch.setenv('DEV_MODE', 'true')
    inspector = inspector_for_item()
    if mismatch == 'table':
        inspector.get_table_names.return_value = ['other']
    elif mismatch == 'column':
        inspector.get_columns.return_value.pop()
    elif mismatch == 'nullable':
        inspector.get_columns.return_value[-1]['nullable'] = False
    elif mismatch == 'primary_key':
        inspector.get_pk_constraint.return_value = {'constrained_columns': []}
    elif mismatch == 'foreign_key':
        inspector.get_foreign_keys.return_value = [{'constrained_columns': ['name'], 'referred_table': 'other', 'referred_columns': ['id']}]
    elif mismatch == 'unique':
        inspector.get_unique_constraints.return_value = [{'column_names': ['name']}]
    else:
        from sqlalchemy import Integer
        inspector.get_columns.return_value[-1]['type'] = Integer()
    with patch('src.utils.managers.database_manager.inspect', return_value=inspector):
        with pytest.raises(Exception):
            manager.validate_schema()


def test_preflight_accepts_matching_schema_without_ddl(database):
    manager, _, statements = database
    with patch('src.utils.managers.database_manager.inspect', return_value=inspector_for_item()):
        manager.validate_schema()
    assert statements == []


def test_lazy_client_constructs_once_when_concurrent_callers_arrive():
    factory = Mock(return_value=Mock(identity='client'))
    client = LazyClient(factory)
    assert factory.call_count == 0
    with ThreadPoolExecutor(8) as pool:
        assert list(pool.map(lambda _: client.identity, range(20))) == ['client'] * 20
    assert factory.call_count == 1


@pytest.mark.parametrize('fail', [False, True])
@pytest.mark.parametrize('image_kind', ['tag', 'digest', 'untagged'])
def test_deployment_uses_same_digest_and_gates_service_update(tmp_path, fail, image_kind):
    fake = tmp_path / 'gcloud'
    fake.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CALL_LOG"
case "$*" in
  "artifacts docker tags list"*) printf '%s\\n' "sha256:$(printf 'a%.0s' {1..64})" ;;
  "run jobs execute"*) if [[ "$FAIL_PREFLIGHT" == '1' ]]; then exit 1; fi ;;
esac
''')
    fake.chmod(0o700)
    log = tmp_path / 'calls'
    digest = 'registry/api@sha256:' + 'a' * 64
    image = {'digest': digest, 'tag': 'registry/api:tag', 'untagged': 'registry/api'}[image_kind]
    result = subprocess.run(['bash', 'dev/deploy_api.sh', image, 'commit'], env={**os.environ, 'PATH': f'{tmp_path}:{os.environ["PATH"]}', 'CALL_LOG': str(log), 'FAIL_PREFLIGHT': '1' if fail else '0'}, capture_output=True, text=True)
    calls = log.read_text().splitlines()
    lookups = [call for call in calls if call.startswith('artifacts ')]
    assert len(lookups) == (0 if image_kind == 'digest' else 1)
    if lookups:
        assert lookups[0].startswith('artifacts docker tags list registry/api ')
        expected_tag = 'latest' if image_kind == 'untagged' else 'tag'
        assert f'--filter=tag.basename()={expected_tag}' in lookups[0]
    assert any('run jobs deploy' in call and '--command=python --args=-m,dev.schema_preflight' in call and f'--image={digest}' in call for call in calls)
    assert any('run jobs delete' in call for call in calls)
    updates = [call for call in calls if 'run services update' in call]
    if fail:
        assert result.returncode != 0
        assert updates == []
    else:
        assert result.returncode == 0, result.stderr
        assert len(updates) == 1
        assert f'--image={digest}' in updates[0]


@pytest.mark.parametrize('resolution', ['missing', 'malformed', 'multiple', 'denied'])
def test_failed_digest_resolution_never_creates_job_or_updates_service(tmp_path, resolution):
    fake = tmp_path / 'gcloud'
    fake.write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CALL_LOG"
case "$RESOLUTION" in
  missing) exit 0 ;;
  malformed) echo 'sha256:not-a-digest' ;;
  multiple) printf 'sha256:%s\\n' "$(printf 'a%.0s' {1..64})" "$(printf 'b%.0s' {1..64})" ;;
  denied) echo 'Artifact Registry permission denied' >&2; exit 1 ;;
esac
''')
    fake.chmod(0o700)
    log = tmp_path / 'calls'
    result = subprocess.run(['bash', 'dev/deploy_api.sh', 'registry/api:tag'], env={**os.environ, 'PATH': f'{tmp_path}:{os.environ["PATH"]}', 'CALL_LOG': str(log), 'RESOLUTION': resolution}, capture_output=True, text=True)
    assert result.returncode != 0
    assert len(log.read_text().splitlines()) == 1
    assert 'run ' not in log.read_text()
    if resolution == 'denied':
        assert 'Artifact Registry permission denied' in result.stderr
    else:
        assert 'Could not resolve an immutable candidate image digest' in result.stderr
