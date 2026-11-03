import csv
import pytest
import yaml
from dm_operators.observers.inventory import compare, index_rows
from dm_operators.results import observations


def model(tmp_path):
    d=tmp_path/'crd/mic-sets';d.mkdir(parents=True)
    (d/'prediction.yaml').write_text(yaml.safe_dump({'id':'micset.predictions','mics':['KLSH']}))
    e=tmp_path/'evidence/input-assets';e.mkdir(parents=True)
    (e/'inventory_source.csv').write_text('MIC,REFERENCE_PREDICTIONS\nKLSH,2025-01-01\n')
    return tmp_path


def test_discrepancies_are_not_automatic_spec_changes(tmp_path):
    root=model(tmp_path)
    before=(root/'crd/mic-sets/prediction.yaml').read_bytes()
    result=compare([{'MIC':'DKEX','Predictions':True,'ReferencePredictions':'2026-09-01'},
                    {'MIC':'KLSH','Predictions':True,'ReferencePredictions':'2025-02-01'}],root)
    assert {'type':'inventory_venue_missing_from_model','mic':'DKEX'} in result['differences']
    assert any(d['type']=='inventory_class_missing_from_model' and d['mic']=='DKEX' for d in result['differences'])
    assert any(d['type']=='inventory_evidence_changed' and d['column']=='referencepredictions' for d in result['differences'])
    assert (root/'crd/mic-sets/prediction.yaml').read_bytes()==before


@pytest.mark.parametrize('rows',[[],[{'MIC':'A'},{'MIC':'a'}],[{'MIC':None}]])
def test_invalid_inventory_is_not_a_clean_comparison(rows):
    with pytest.raises(ValueError): index_rows(rows)


def test_missing_flag_column_is_unknown(tmp_path):
    report=compare([{'MIC':'KLSH'}],model(tmp_path))
    assert {'type':'inventory_column_missing','column':'predictions'} in report['differences']
    assert not any(d['type']=='declared_class_not_available_in_inventory' for d in report['differences'])


def test_status_publication():
    doc={'kind':'InventoryReconciliationStatus','id':'status.inventory.snowflake','checked_at':'2026-11-03T00:00:00Z','difference_count':2}
    assert observations([{'path':'observations/inventory-status.yaml','content':yaml.safe_dump(doc)}],'2026-11-03T00:00:00Z')[0]['document']['difference_count']==2
