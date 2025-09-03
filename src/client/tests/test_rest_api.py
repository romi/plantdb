import unittest

from plantdb.client.rest_api import archive_url
from plantdb.client.rest_api import base_url
from plantdb.client.rest_api import refresh_url
from plantdb.client.rest_api import sanitize_name
from plantdb.client.rest_api import scan_image_url
from plantdb.client.rest_api import scan_url
from plantdb.client.rest_api import scans_url


class TestRestApi(unittest.TestCase):

    def test_sanitize_name(self):
        self.assertEqual(sanitize_name("  example  "), "example")
        self.assertEqual(sanitize_name("path/to/example"), "example")
        self.assertEqual(sanitize_name("valid-name_123"), "valid-name_123")
        with self.assertRaises(ValueError):
            sanitize_name("invalid@name")
        with self.assertRaises(ValueError):
            sanitize_name("")
        with self.assertRaises(ValueError):
            sanitize_name("/////////")

    def test_base_url(self):
        self.assertEqual(base_url('localhost', 2020, prefix=''),
                         'http://localhost:2020')
        self.assertEqual(base_url('localhost', 2020, prefix='', ssl=True),
                         'https://localhost:2020')
        self.assertEqual(base_url('localhost', 2020, prefix='/plantdb'),
                         'http://localhost/plantdb/')

    def test_scans_url(self):
        self.assertEqual(scans_url(host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/scans')
        self.assertEqual(scans_url(host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/scans')

    def test_scan_url(self):
        self.assertEqual(scan_url('example', host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/scans/example')
        self.assertEqual(scan_url('example', host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/scans/example')

    def test_scan_image_url(self):
        self.assertEqual(scan_image_url('example', 'images', '0', host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/image/example/images/0?size=orig')
        self.assertEqual(
            scan_image_url('example', 'images', '1', 'big', host='localhost', port=2020, prefix=''),
            'http://localhost:2020/image/example/images/1?size=big')
        self.assertEqual(scan_image_url('example', 'images', '0', host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/image/example/images/0?size=orig')
        self.assertEqual(
            scan_image_url('example', 'images', '1', 'big', host='localhost', port=2020, prefix='/plantdb'),
            'http://localhost/plantdb/image/example/images/1?size=big')

    def test_refresh_url(self):
        self.assertEqual(refresh_url(host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/refresh')
        self.assertEqual(refresh_url('example', host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/refresh?scan_id=example')
        self.assertEqual(refresh_url(host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/refresh')
        self.assertEqual(refresh_url('example', host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/refresh?scan_id=example')

    def test_archive_url(self):
        self.assertEqual(archive_url('real_plant', host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/archive/real_plant')
        self.assertEqual(archive_url('real_plant', host='localhost', port=2020, prefix='/plantdb'),
                         'http://localhost/plantdb/archive/real_plant')


if __name__ == '__main__':
    unittest.main()
