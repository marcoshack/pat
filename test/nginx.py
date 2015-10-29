import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

import unittest
import shutil
import os.path
import glob
import pat.nginx as nginx

class TestNginxMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.reqs = nginx.load_access_log_data('test/fixtures/nginx_test.csv', save_binary=False)

    @classmethod
    def tearDownClass(cls):
        # just in case someone forget to use load_access_log_data(..., save_binary=False)
        for path in glob.glob('test/fixtures/*.gl'):
            logger.info('Removing directory %s', path)
            shutil.rmtree(path)

    def test_all_lines_are_loaded(self):
        self.assertEqual(len(self.reqs), 100)

    def test_data_is_enriched(self):
        req = self.reqs[0]
        self.assertEqual(self.reqs[0]['method']  , 'HEAD')
        self.assertEqual(self.reqs[0]['base_url'], '/example/qux')
        self.assertEqual(self.reqs[0]['error']   , 0)
        self.assertEqual(self.reqs[1]['method']  , 'GET')
        self.assertEqual(self.reqs[1]['base_url'], '/example/foo/bar/')
        self.assertEqual(self.reqs[1]['error']   , 0)

    def test_aggregate_rps(self):
        rps = nginx.aggregate_rps(self.reqs)
        self.assertEquals(rps['count'].sum(), 100)
        self.assertEquals(rps['successes'].sum(), 96)
        self.assertEquals(rps['errors'].sum(), 4)

    def test_summary_with_no_start_date_and_period(self):
        reqs, rps = nginx.summary(self.reqs, stats=False)
        self.assertEquals(len(reqs), len(self.reqs))
        self.assertEquals(len(rps), 41)

    def test_errors_by_host(self):
        err_by_host = nginx.errors_by_host(self.reqs)
        self.assertEquals(err_by_host[0]['err_pct'], 100.0)

    def test_errors_by_host_with_no_errors(self):
        no_err_reqs = nginx.load_access_log_data('test/fixtures/nginx_test-no_err.csv', save_binary=False)
        err_by_host = nginx.errors_by_host(no_err_reqs)
        self.assertEquals(err_by_host[0]['err_pct'], 0)

    def test_find_load_periods_without_loads(self):
        rps = nginx.aggregate_rps(self.reqs)
        periods = nginx.find_load_periods(rps)
        self.assertEquals(len(periods), 0)

    def test_find_load_periods_with_loads(self):
        reqs = nginx.load_access_log_data('test/fixtures/nginx_test-with_loads.csv', save_binary=False)
        reqs, rps = nginx.summary(reqs)
        periods = nginx.find_load_periods(rps, surrounding_period=5, rps_threashold=20, load_pause_period=5)
        self.assertEquals(len(periods), 2)
        self.assertEquals(periods[0].start, 1444359676)
        self.assertEquals(periods[0].end, 1444359688)
        self.assertEquals(periods[0].duration(), 12)
        self.assertEquals(periods[1].start, 1444359716)
        self.assertEquals(periods[1].end, 1444359726)
        self.assertEquals(periods[1].duration(), 10)

if __name__ == '__main__':
    unittest.main()
