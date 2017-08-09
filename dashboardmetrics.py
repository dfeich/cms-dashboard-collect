import requests
import pandas as pd
from io import StringIO

def get_siteconsumption(qtype, start, end, colname='values', site='T2_CH_CSCS'):
    baseurl = 'http://dashb-cms-job-dev.cern.ch/dashboard/request.py/'
    queryurl = baseurl + 'consumptionscsv'
    params = {
	'sites': site,
	'datatiers':'All DataTiers',
	'activities':'all',
	'sitesSort':'2',
	'start': start,
	'end': end,
	'timeRange':'daily',
	'granularity':'Daily',
	'generic':'0',
	'sortBy':'1',
	'series':'All',
	'type': qtype
    }
    res = requests.get(queryurl, params=params)
    df = pd.read_csv(StringIO(res.text), index_col=1,
		     header=None, names=[colname])
    df.index.name = 'jobtype'
    return df

def get_goodjob_cpu(start, end, site='T2_CH_CSCS'):
    return get_siteconsumption('ewgb', start, end, 'good_cpu_s', site)

def get_alljob_cpu(start, end, site='T2_CH_CSCS'):
    return get_siteconsumption('ewab', start, end,'all_cpu_s', site)

def get_goodjob_wallt(start, end, site='T2_CH_CSCS'):
    return get_siteconsumption('ewg', start, end,'good_wallt_s', site)

def get_alljob_wallt(start, end, site='T2_CH_CSCS'):
    return get_siteconsumption('ewa', start, end,'all_wallt_s', site)

def get_metricstable(start, end, site='T2_CH_CSCS'):
    df = pd.concat([get_goodjob_cpu(start, end, site),
		    get_alljob_cpu(start, end, site),
		    get_goodjob_wallt(start, end, site),
		    get_alljob_wallt(start, end, site)],
		   axis=1, join='outer')
    # we want hours, not seconds
    df = df / 3600
    df = df.round(1)
    df.rename(columns={'good_wallt_s' : 'good_wallt_h',
		       'good_cpu_s' : 'good_cpu_h',
		       'all_wallt_s' : 'all_wallt_h',
		       'all_cpu_s' : 'all_cpu_h'}, inplace=True)

    df = df.append(df.sum().rename('ALL_JOBS', inplace=True))
    df['good_wallt_%'] = 100 * df['good_wallt_h'] / df['all_wallt_h']
    df['good_cpu_eff_%'] = 100 * df['good_cpu_h'] / df['all_cpu_h']
    df = df.round({'good_wallt_%' : 1,
		   'good_cpu_eff_%' : 1})
    return df
