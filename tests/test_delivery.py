import datetime as dt
import pytest
from dm_operators.observers.delivery import estimate, percentile


def rows(hours):
    date=dt.date(2026,10,1)
    return [{'MIC':'XCME','TRADE_DATE':date+dt.timedelta(days=i),'LAST_MODIFIED':dt.datetime.combine(date+dt.timedelta(days=i),dt.time(),tzinfo=dt.timezone.utc)+dt.timedelta(hours=h)} for i,h in enumerate(hours)]


def test_quantile_respects_day_offset_and_rounds_up():
    times,samples,errors=estimate(rows([23,25]),['XCME'],2)
    assert not errors
    assert times==[{'mic':'XCME','landing_time':'00:54:00','day':'T+1'}]
    assert samples[0]['sample_days']==2


def test_insufficient_data_does_not_invent_an_estimate():
    times,_,errors=estimate(rows([25]),['XCME','XEUR'],2)
    assert times==[] and len(errors)==2


def test_late_backfills_are_not_silently_dropped():
    times,samples,errors=estimate(rows([25,100]),['XCME'],2)
    assert times[0]['day']=='T+4' and not errors and samples[0]['days_over_48h']==1


def test_rejects_timezone_ambiguity():
    data=rows([25]);data[0]['LAST_MODIFIED']=dt.datetime(2026,10,2)
    with pytest.raises(ValueError,match='timezone'):estimate(data,['XCME'],1)
