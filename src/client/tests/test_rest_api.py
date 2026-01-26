import unittest

from plantdb.client.api_endpoints import sanitize_name
from plantdb.client.rest_api import archive_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import refresh_url
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
        self.assertEqual(plantdb_url('localhost', 2020, prefix=''),
                         'http://localhost:2020')
        self.assertEqual(plantdb_url('localhost', 2020, prefix='', ssl=True),
                         'https://localhost:2020')
        self.assertEqual(plantdb_url('localhost', prefix='/plantdb'),
                         'http://localhost/plantdb/')

    def test_scans_url(self):
        self.assertEqual(scans_url('localhost', port=2020, prefix=''),
                         'http://localhost:2020/scans')
        self.assertEqual(scans_url('localhost', prefix='/plantdb'),
                         'http://localhost/plantdb/scans')

    def test_scan_url(self):
        self.assertEqual(scan_url('localhost', 'example', port=2020, prefix=''),
                         'http://localhost:2020/scans/example')
        self.assertEqual(scan_url('localhost', 'example', prefix='/plantdb'),
                         'http://localhost/plantdb/scans/example')

    def test_scan_image_url(self):
        self.assertEqual(scan_image_url('localhost', 'example', 'images', '0', port=2020, prefix=''),
                         'http://localhost:2020/image/example/images/0?size=orig')
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '1', 'big', port=2020, prefix=''),
            'http://localhost:2020/image/example/images/1?size=big')
        self.assertEqual(scan_image_url('localhost', 'example', 'images', '0', prefix='/plantdb'),
                         'http://localhost/plantdb/image/example/images/0?size=orig')
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '1', 'big', prefix='/plantdb'),
            'http://localhost/plantdb/image/example/images/1?size=big')

    def test_refresh_url(self):
        self.assertEqual(refresh_url(host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/refresh')
        self.assertEqual(refresh_url('localhost', 'example', port=2020, prefix=''),
                         'http://localhost:2020/refresh?scan_id=example')
        self.assertEqual(refresh_url(host='localhost', prefix='/plantdb'),
                         'http://localhost/plantdb/refresh')
        self.assertEqual(refresh_url('localhost', 'example', prefix='/plantdb'),
                         'http://localhost/plantdb/refresh?scan_id=example')

    def test_archive_url(self):
        self.assertEqual(archive_url('localhost', 'real_plant', port=2020, prefix=''),
                         'http://localhost:2020/archive/real_plant')
        self.assertEqual(archive_url('localhost', 'real_plant', prefix='/plantdb'),
                         'http://localhost/plantdb/archive/real_plant')


if __name__ == '__main__':
    unittest.main()
