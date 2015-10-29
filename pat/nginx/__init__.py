from __future__ import division
from datetime import datetime

import os.path
import shutil
import re
import logging
import sframe
import sframe.aggregate as agg
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

def load_access_log_data(csv_filename, force=False, save_binary=True):
    """ Load access log data from a sframe.SFrame binary with name <csv_filename>.gl - if exists - or from a CSV file named <csv_filename>.csv and than save to a SFrame binary format.
    """
    gl_filename = '%s.gl' % csv_filename

    if os.path.isdir(gl_filename):
        if not force:
            logger.info('Loading SFrame from %s', gl_filename)
            reqs = sframe.SFrame(gl_filename)
            return reqs
        else:
            logger.info('Removing dir %s', gl_filename)
            shutil.rmtree(gl_filename)

    logger.info('Loading data from %s', csv_filename)
    reqs = sframe.SFrame(csv_filename)

    logger.info('Enriching data')
    __enrich_request_data(reqs)

    if save_binary:
        logger.info('Saving data in SFrame binary format to %s', gl_filename)
        reqs.save(gl_filename, format='binary')
    else:
        logger.info('Saving SFrame to binary format skipped')

    return reqs

def plot_rps(rps_aggr, figsize=(15,7)):
    """ Plot the give RPS aggregate (timestamp,count,errors) as a matplotlib.pyplot stacked bar graph.
    """
    plt.figure(1,figsize=figsize);
    ax = plt.subplot(111)
    ax.bar(rps_aggr['timestamp'], rps_aggr['successes'], width=1, color='b')
    ax.bar(rps_aggr['timestamp'], rps_aggr['errors'], width=1, color='r', bottom=rps_aggr['successes'])
    plt.ylabel('# requests/sec')
    plt.xlabel('timestamp (sec)')

def aggregate_rps(reqs):
    """ Creates an RPS aggregate (timestamp,count,errors) from the given requests (sframe.SFrame).
    """
    rps = reqs.groupby(key_columns='timestamp',
                           operations={
                            'count': agg.COUNT(),
                            'errors': agg.SUM('error'),
                            'avg_response_time': agg.AVG('service_time')
                           })
    rps['successes'] = rps[['count','errors']].apply(lambda i: i['count'] - i['errors'])
    return rps.sort('timestamp')

def request_by_timestamp(reqs, start, end):
    """ Create a new request SFrame filtered by timestamp (start, end).
    """
    return reqs[(reqs['timestamp'] >= start) & (reqs['timestamp'] <= end)]

def find_rps_holes(rps_aggr):
    """ Find timestamp holes in the given RPS aggregate.
    """
    ts_to_add = []
    last_ts = rps_aggr[0]['timestamp'] - 1
    for i in rps_aggr:
        expected_ts = last_ts + 1
        if i['timestamp'] > expected_ts:
            for new_ts in range(expected_ts, i['timestamp']): ts_to_add.append(new_ts)
        last_ts = i['timestamp']
    return ts_to_add

def fill_rps_holes(rps_aggr):
    """ Find timestamp holes in the RPS aggregate and fill it with zeroed entries (timestamp,count:0,errors:0)
    """
    holes_ts = find_rps_holes(rps_aggr)
    if len(holes_ts) == 0: return rps_aggr
    zero_col_int = [0]*len(holes_ts)
    new_items = sframe.SFrame({
            'timestamp': holes_ts,
            'count': zero_col_int,
            'errors': zero_col_int,
            'successes': zero_col_int,
            'avg_response_time': [0.0]*len(holes_ts)
            })
    return rps_aggr.append(new_items).sort('timestamp')

def filter(requests, start=None, period=None, stats=True):
    """ Filter requests for the given start+period (or return the same list if None), creates an RPS from requests and return both, displaying statistics about the requests and RPS lists (unless stats=False is given).
    """
    reqs = requests
    if start != None and period != None:
        end  = start + period
        reqs = request_by_timestamp(requests, start, start+period)
    rps = aggregate_rps(reqs)
    rps_full = fill_rps_holes(rps)
    if stats: show_stats(reqs, rps_full)
    return reqs, rps_full

def summary(requests, start=None, period=None, stats=True):
    return filter(requests, start, period, stats)

def show_stats(reqs, rps):
    """ Show handy information about the request list and RPS aggregate.
    """
    start_ts   = rps[1]['timestamp']
    start_date = datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')
    end_ts     = rps[-1]['timestamp']
    end_date   = datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M:%S')
    elapsed    = end_ts - start_ts
    print 'start: %s (%d), end %s (%d), elapsed: %d sec, requests: %d' % (start_date, start_ts, end_date, end_ts, elapsed, len(reqs))

def error_summary(reqs):
    """ Creates an error aggreate from the given request list. Errors are requests that were responded with status_code > 400.
    """
    err_reqs = reqs[(reqs['status_code'] > 400)]
    err_agg = err_reqs.groupby(key_columns=['status_code','method','base_url'], operations={'count': agg.COUNT()})
    total_reqs = err_reqs['error'].sum()
    err_agg['percent'] = err_agg['count'].apply(_error_pct_lambda(total_reqs))
    err_agg = err_agg.sort('count', ascending=False)
    return err_agg

def errors_by_host(reqs, host_column='log_file', error_column='error', agg_error_column='errors',
                   agg_count_column='count', agg_err_pct_column='err_pct'):
    """ Creates an aggregate with the number of errors by distinct log_file (that usualy represents a host).
    """
    reqs_total = len(reqs)
    reqs_by_host = reqs.groupby(
        key_columns=[host_column],
        operations={
            agg_count_column: agg.COUNT(),
            agg_error_column: agg.SUM(error_column)
        })
    total_errors = reqs_by_host[agg_error_column].sum()
    reqs_by_host[agg_err_pct_column] = reqs_by_host[agg_error_column].apply(_error_pct_lambda(total_errors))
    return reqs_by_host

def _error_pct_lambda(total_reqs):
    if total_reqs > 0:
        return lambda n: (n/total_reqs)*100
    else:
        return lambda n: 0

def __extract_single_value(regexp, req_line):
    m = re.search(regexp, req_line)
    if m != None: return m.group(1)

def __extract_http_method(req_line):
    """ Extracts the HTTP method from the given request line.
    >>> __extract_http_method('GET /example HTTP/1.1')
    'GET'
    >>> __extract_http_method('HEAD /example/foo?attr=value&attr=value HTTP/1.1')
    'HEAD'
    """
    return __extract_single_value('^([A-Z]+) .*', req_line)

def __extract_base_url(req_line):
    """ Extracts the base URL (without query params) from the given request line.
    >>> __extract_base_url('GET /example HTTP/1.1')
    '/example'
    >>> __extract_base_url('HEAD /example/foo HTTP/1.1')
    '/example/foo'
    >>> __extract_base_url('HEAD /example/foo?attr=value&attr=value HTTP/1.1')
    '/example/foo'
    """
    return __extract_single_value('^[A-Z]+ (\/[a-zA-Z_\-\/]*)[\?\s]{1}.*', req_line)

def __enrich_request_data(reqs, columns=('method','base_url','error')):
    if 'method'   in columns: reqs['method']   = reqs['request_line'].apply(__extract_http_method)
    if 'base_url' in columns: reqs['base_url'] = reqs['request_line'].apply(__extract_base_url)
    if 'error'    in columns: reqs['error']    = reqs['status_code'].apply(lambda code: code >= 400)
    return reqs

class LoadPeriod:
    def __init__(self, start=None):
        self.start = start
        self.end = None

    def duration(self):
        if self.is_closed():
            return self.end - self.start
        else:
            return 0

    def is_started(self):
        return self.start != None

    def is_open(self):
        return self.start != None and self.end == None

    def is_closed(self):
        return self.start != None and self.end != None

    def __str__(self):
        return 'LoadPeriod(start: %d, end: %d, duration: %d)' % (self.start or 0, self.end or 0, self.duration())

def find_load_periods(rps_aggr, surrounding_period=120, rps_threashold=300, load_pause_period=60):
    load_periods = []
    current_load = LoadPeriod()
    pause_count = 0
    for i in rps_aggr:
        if i['count'] >= rps_threashold:
            if not current_load.is_started():
                current_load.start = i['timestamp'] - surrounding_period # start load
            else:
                pause_count = 0 # reset pause_count
        elif i['count'] < rps_threashold and current_load.is_started():
            pause_count += 1

        if current_load.is_started() and pause_count >= load_pause_period: # end load
            current_load.end = i['timestamp']
            load_periods.append(current_load)
            pause_count = 0
            current_load = LoadPeriod()

    if current_load.is_open():
        current_load.end = rps[-1]['timestamp']
        load_periods.append(current_load)

    return load_periods


if __name__ == '__main__':
    import doctest
    doctest.testmod()
