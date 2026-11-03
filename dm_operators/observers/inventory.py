"""Compare live inventory with declared venue sets and the model's inventory evidence.

Inventory is observational evidence, not authority to change declarations. Reports
separate undeclared venues, declared venues absent from inventory, asset-class
membership differences, and changed inventory fields. Failed or empty reads fail the run. Missing/duplicate MICs are reported as
source-quality differences; ambiguous rows never choose a winner or imply absence.
"""
import argparse
import datetime as dt
import csv
import json
import re
from pathlib import Path
import yaml
from .warehouse import connect


def normalize(value):
    return re.sub(r'[^a-z0-9]', '', str(value).lower())


def index_rows(rows):
    indexed = {}
    for raw in rows:
        row = {normalize(k): (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in raw.items()}
        mic = str(row.get('mic') or '').strip().upper()
        if not mic or mic in indexed:
            raise ValueError('Inventory has a missing or duplicate MIC')
        indexed[mic] = row
    if not indexed:
        raise ValueError('Inventory is empty; refusing to infer missing venues')
    return indexed


def compare(rows, root):
    if not rows:
        raise ValueError('Inventory is empty; refusing to infer missing venues')
    grouped = {}
    source_issues = []
    for i, raw in enumerate(rows):
        normalized = {normalize(k): v for k, v in raw.items()}
        mic = str(normalized.get('mic') or '').strip().upper()
        if not mic:
            source_issues.append({'type':'inventory_row_without_mic', 'row_index':i, 'exchange':normalized.get('exchange')})
        else:
            grouped.setdefault(mic, []).append(raw)
    ambiguous = {m for m, group in grouped.items() if len(group)>1}
    source_issues += [{'type':'inventory_duplicate_mic', 'mic':m, 'row_count':len(grouped[m])} for m in sorted(ambiguous)]
    valid = [group[0] for mic, group in grouped.items() if mic not in ambiguous]
    if not valid:
        raise ValueError('Inventory has no unambiguous venue rows')
    live = index_rows(valid)
    sets = {}
    for p in sorted((root / 'crd/mic-sets').glob('*.yaml')):
        doc = yaml.safe_load(p.read_text())
        sets[doc['id']] = {str(m).upper() for m in doc.get('mics', [])}
    known = set().union(*sets.values()) if sets else set()
    if not known: raise ValueError('Model has no venue declarations')
    differences = source_issues + [{'type': 'inventory_venue_missing_from_model', 'mic': m} for m in sorted(live.keys() - known)]
    differences += [{'type': 'declared_venue_absent_from_inventory', 'mic': m} for m in sorted(known - live.keys() - ambiguous)]
    # Flags express the inventory's class-level availability. Absence of a flag
    # column is a schema difference, never interpreted as false availability.
    for flag, sid in [('equity','micset.global-equity'), ('futures','micset.futures'), ('options','micset.options'), ('predictions','micset.predictions')]:
        if sid not in sets: continue
        if not all(flag in row for row in live.values()):
            differences.append({'type':'inventory_column_missing', 'column':flag}); continue
        observed = {m for m,r in live.items() if str(r[flag]).lower() == 'true'}
        # The model deliberately separates OPRA from non-OPRA options.
        if flag == 'options': observed -= sets.get('micset.opra', set())
        for m in sorted(observed - sets[sid]): differences.append({'type':'inventory_class_missing_from_model', 'mic':m, 'mic_set':sid})
        for m in sorted(sets[sid] - observed - ambiguous): differences.append({'type':'declared_class_not_available_in_inventory', 'mic':m, 'mic_set':sid})
    evidence = root / 'evidence/input-assets/inventory_source.csv'
    if evidence.exists():
        with evidence.open(newline='') as f: previous = index_rows(list(csv.DictReader(f)))
        for mic in sorted(live.keys() & previous.keys()):
            for col in sorted(live[mic].keys() & previous[mic].keys() - {'mic'}):
                old, new = previous[mic][col], live[mic][col]
                canon = lambda v: '' if v is None else str(v).strip().lower()
                if canon(old) != canon(new):
                    differences.append({'type':'inventory_evidence_changed', 'mic':mic, 'column':col, 'model_evidence':old, 'inventory':new})
        for col in sorted(set().union(*(set(r) for r in live.values())) - set().union(*(set(r) for r in previous.values()))):
            differences.append({'type':'inventory_column_missing_from_evidence','column':col})
    return {'inventory_rows':len(rows), 'ambiguous_venues':sorted(ambiguous), 'inventory_venues':len(live), 'declared_venues':len(known), 'differences':differences}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backend', choices=['snowflake','databricks'], default='snowflake')
    p.add_argument('--table', default='PROD_COEF_DB.PUBLIC.INVENTORY')
    args=p.parse_args()
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}',args.table): raise ValueError('Inventory must be a three-part SQL identifier')
    with connect(args.backend) as connection:
        with connection.cursor() as cur:
            cur.execute('SELECT * FROM '+args.table+' LIMIT 10001')
            rows=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
    if len(rows)>10000: raise ValueError('Inventory exceeds the bounded 10000 venue limit')
    out=Path('observations');out.mkdir(exist_ok=True)
    (out/'inventory-source.json').write_text(json.dumps(rows,default=str,indent=2))
    report=compare(rows,Path.cwd())
    report.update(source=args.table,backend=args.backend,checked_at=dt.datetime.now(dt.timezone.utc).isoformat())
    (out/'inventory-reconciliation.json').write_text(json.dumps(report,default=str,indent=2))
    doc={'kind':'InventoryReconciliationStatus','id':'status.inventory.'+args.backend,'subject':'inventory',
         'checked_at':report['checked_at'],'source':args.table,'difference_count':len(report['differences']),
         'inventory_venues':report['inventory_venues'],'declared_venues':report['declared_venues'],
         'differences':report['differences']}
    (out/'inventory-status.yaml').write_text(yaml.safe_dump(doc,sort_keys=False))
    print(json.dumps(report,default=str,indent=2))

if __name__=='__main__': main()
