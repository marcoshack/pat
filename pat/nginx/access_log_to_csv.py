import logging
import time
import os
import sys
import re
import threading

from datetime import datetime
from time import strptime, mktime

log = logging.getLogger(__file__)
log.setLevel('DEBUG')

def main(filenames):
    _print_csv_header()
    start = time.time()
    for fname in filenames:
        _parse_file(fname)
    log.info('Done in %d seconds', time.time() - start)

def _print_csv_header():
    print 'log_file,source_ip,username,date,timestamp,host,request_line,status_code,service_time,upstream_service_time'

def _parse_file(filename):
    start = time.time()
    line_count = 0
    f = open(filename, 'r')
    for l in f:
        m = re.search('([0-9\.]+) - ([A-Za-z0-9_-]+) \[(.*)\] "([a-zA-Z0-9\-\.]+)" "([A-Za-z0-9\/\.\-\s\?\&\=_]+)" ([0-9]{3}) [0-9]+ .* "service_time: ([0-9\.-]+)" "upstream_service_time: ([0-9\.-]+)"', l)
        if m != None:
            _print_csv_line(filename, m)
            line_count += 1
        else:
            log.warn('no matched line: "%s"', l)
    elapsed = time.time()-start
    log.info('%d parsed lines from %s in %.2f seconds (~%d lines/s)', line_count, filename, elapsed, line_count/elapsed)
    f.close

def _print_csv_line(filename, m):
    timestamp = _parse_timestamp(m.group(3), '%d/%b/%Y:%H:%M:%S -0200')
    print '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
        os.path.basename(filename),
        m.group(1), # source_ip
        m.group(2), # username
        datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S-02:00'),
        int(timestamp),
        m.group(4), # host
        m.group(5), # request_line
        m.group(6), # status_code
        m.group(7), # service_time
        m.group(8)) # upstream_service_time

def _parse_timestamp(str_date, format):
    """ Parse string date in the format DD/MMM/YYYY:HH:mm:ss, e.g.: 26/Oct/2015:00:01:04
    >>> _parse_timestamp('26/Oct/2015:15:32:55 -0200', '%d/%b/%Y:%H:%M:%S -0200')
    1445880775.0
    """
    return mktime(strptime(str_date, format))

def usage():
    print '\nUsage: %s <filename_pattern>\n' % __file__

if __name__ == '__main__':
    if 'DOCTEST' in os.environ:
        import doctest
        doctest.testmod()
        exit(0)

    if len(sys.argv) >= 2:
        main(sys.argv[1:])
    else:
        usage()
