"""The publisher preserves failed status objects and completes only after uploads."""
import json
from pathlib import Path

import pytest
from dm_operators import results


def test_status_documents_and_errors_are_preserved():
    files = [{'path': 'crd/status/x.yaml', 'content': 'kind: MaterialisationStatus\nid: status.x\nsubject: equity.l2\nerror: connection failed\n'},
             {'path': 'crd/assets/x.yaml', 'content': 'kind: DataAsset\nid: equity.l2\n'}]
    obs = results.observations(files, '2026-11-03T12:00:00Z')
    assert len(obs) == 1 and obs[0]['error'] == 'connection failed'
    assert obs[0]['subject'] == 'equity.l2'


def test_artifacts_uploaded_before_completion(tmp_path, monkeypatch):
    (tmp_path / 'test.sql').write_text('SELECT 1;')
    calls = []
    monkeypatch.setattr(results, 'call', lambda method, path, payload=None, file=None: calls.append((method, path, payload, file)))
    value = dict(status='succeeded', files=[], artifacts=['test.sql'], finished_at='2026-11-03T12:00:00Z')
    url = results.publish({'id': 'run-tests-001'}, value, tmp_path)
    assert calls[-1][0] == 'POST' and calls[-1][1].endswith('/complete')
    assert len(calls) == 3 and all(c[0] == 'PUT' for c in calls[:-1])
    assert url.endswith('/results?run=run-tests-001')
    assert json.loads((tmp_path / '_result/result.json').read_text()) == value


def test_missing_artifact_does_not_publish_success(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(results, 'call', lambda *a, **kw: calls.append(a))
    with pytest.raises(ValueError, match='missing'):
        results.publish({'id': 'run-tests-001'}, dict(status='succeeded', artifacts=['missing.sql']), tmp_path)
    assert not any(c[0] == 'POST' for c in calls)


def test_existing_status_files_infer_kind_from_the_model_directory():
    files = [{'path': 'crd/status/equity.l2.yaml', 'content': 'id: status.l2\nsubject: equity.l2\nchecked_at: 2026-11-03T12:00:00Z\n'},
             {'path': 'crd/delivery-status/options.trades.yaml', 'content': 'id: status.delivery.options.trades\nproduct_id: product-options-trades\n'},
             {'path': 'crd/assets/ignored.yaml', 'content': 'id: equity.l2\n'}]
    obs = results.observations(files, '2026-11-03T12:00:00Z')
    assert [o['kind'] for o in obs] == ['MaterialisationStatus', 'DeliveryTimeStatus']
    assert obs[1]['subject'] == 'product-options-trades'
