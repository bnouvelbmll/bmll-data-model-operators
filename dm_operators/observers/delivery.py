"""Measure options file completion estimates from production external-table metadata.

For each dataset/MIC/trade date, the last S3 modification among registered files
is the completion proxy. The p95 is calculated over elapsed UTC seconds since
trade-date midnight, not time-of-day (which would misorder midnight crossings).
This is an estimate from current object metadata, not an initial-delivery audit:
rewrites and backfills can increase it, and unregistered/missing files are unknown.
No late observations are silently excluded. Calendar-day offsets retain delays
beyond T+1; they must not be interpreted as contractual delivery commitments.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import yaml
from .warehouse import connect

DATASETS = {
    'Option-Reference': ('OPTION_REFERENCE', 'product.options-reference'),
    'Option-L1': ('OPTION_L1', 'product.options-l1'),
    'Option-MarketState': ('OPTION_MARKET_STATE', 'product.options-market-state'),
    'Option-Trades': ('OPTION_TRADES', 'product.options-trades'),
    'Option-Statistics': ('OPTION_STATISTICS', 'product.options-statistics'),
}


def percentile(values, fraction=.95):
    """Linear interpolated quantile, matching PERCENTILE_CONT; round up later."""
    values=sorted(values)
    if not values: raise ValueError('No observations')
    rank=(len(values)-1)*fraction
    low=math.floor(rank);high=math.ceil(rank)
    return values[low]+(values[high]-values[low])*(rank-low)


def estimate(rows, mics, minimum):
    by_mic={m:[] for m in mics}
    for row in rows:
        mic=row['MIC']
        if mic not in by_mic: continue
        date=dt.date.fromisoformat(str(row['TRADE_DATE'])[:10])
        stamp=row['LAST_MODIFIED']
        if not isinstance(stamp,dt.datetime): stamp=dt.datetime.fromisoformat(str(stamp))
        if stamp.tzinfo is None: raise ValueError('Arrival timestamps must have a timezone')
        elapsed=(stamp-dt.datetime.combine(date,dt.time(),tzinfo=dt.timezone.utc)).total_seconds()
        if elapsed<0: raise ValueError('Arrival predates trade date')
        by_mic[mic].append(elapsed)
    times=[]; summaries=[]; errors=[]
    for mic,values in sorted(by_mic.items()):
        summary={'mic':mic,'sample_days':len(values)}
        if len(values)<minimum:
            errors.append(f'{mic}: {len(values)} observed days; at least {minimum} required')
        else:
            seconds=math.ceil(percentile(values));summary['p95_elapsed_seconds']=seconds
            summary['days_over_48h']=sum(v>=172800 for v in values)
            day,clock=divmod(seconds,86400);hours,clock=divmod(clock,3600);minutes,seconds=divmod(clock,60)
            times.append({'mic':mic,'landing_time':f'{hours:02d}:{minutes:02d}:{seconds:02d}','day':'T' if day==0 else f'T+{day}'})
        summaries.append(summary)
    return times,summaries,errors


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--days',type=int,default=30,help='Completed trade-date window ending yesterday, 1–90 calendar days.')
    p.add_argument('--minimum-samples',type=int,default=10,help='Minimum observed venue days required per dataset.')
    args=p.parse_args()
    if not 1<=args.days<=90 or not 1<=args.minimum_samples<=args.days: raise ValueError('Invalid measurement window/sample count')
    now=dt.datetime.now(dt.timezone.utc);end=now.date()-dt.timedelta(days=1);start=end-dt.timedelta(days=args.days-1)
    root=Path.cwd();mics=yaml.safe_load((root/'crd/mic-sets/micset-options.yaml').read_text())['mics']
    out=root/'observations';out.mkdir(exist_ok=True)
    statuses=root/'crd/delivery-status';statuses.mkdir(exist_ok=True)
    report={'checked_at':now.isoformat(),'window_start':str(start),'window_end':str(end),'method':__doc__,'datasets':[]}
    with connect('snowflake') as connection:
        for name,(table,product) in DATASETS.items():
            if not (root/'crd/products'/ (product.replace('.','-',1)+'.yaml')).exists(): raise ValueError(f'Model is missing {product}')
            source='PROD_L2_DATA.PUBLIC.'+table
            query=f"""SELECT SPLIT_PART(FILE_NAME,'/',2) MIC,
 TRY_TO_DATE(CONCAT_WS('-',SPLIT_PART(FILE_NAME,'/',3),SPLIT_PART(FILE_NAME,'/',4),SPLIT_PART(FILE_NAME,'/',5))) TRADE_DATE,
 COUNT(*) FILES, MAX(LAST_MODIFIED) LAST_MODIFIED
 FROM TABLE(PROD_L2_DATA.INFORMATION_SCHEMA.EXTERNAL_TABLE_FILES(TABLE_NAME => '{source}'))
 WHERE TRADE_DATE BETWEEN '{start}' AND '{end}' GROUP BY 1,2 ORDER BY 1,2"""
            with connection.cursor() as cur:
                cur.execute(query);rows=[dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]
            content=json.dumps(rows,default=str,indent=2)
            (out/(name+'.json')).write_text(content)
            times,samples,errors=estimate(rows,mics,args.minimum_samples)
            report['datasets'].append({'dataset':name,'source':source,'samples':samples,'undeclared_mics':sorted({r['MIC'] for r in rows}-set(mics)),'errors':errors,'landing_times':times,'query':query})
            slug=name.lower()
            doc={'id':'status.delivery.options.'+slug,'dataset_type':'options','dataset_name':name,'product_id':product,
                 'statistic':'p95','time_zone':'UTC','landing_times':times,'source':source+' via EXTERNAL_TABLE_FILES; '+str(start)+' through '+str(end),
                 'source_sha256':hashlib.sha256(content.encode()).hexdigest(),'checked_at':now.isoformat(),'checked_by':'measure-delivery-times'}
            if errors:doc['error']='; '.join(errors)
            (statuses/('options.'+slug+'.yaml')).write_text(yaml.safe_dump(doc,sort_keys=False))
            print(f'{name}: {len(times)}/{len(mics)} venue estimates; {len(errors)} errors',flush=True)
    (out/'delivery-report.json').write_text(json.dumps(report,default=str,indent=2))
    print(json.dumps(report,default=str,indent=2))
    return 1 if any(d['errors'] for d in report['datasets']) else 0

if __name__=='__main__': raise SystemExit(main())
