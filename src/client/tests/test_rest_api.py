import os
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from plantdb.client.rest_api import api_token_url
from plantdb.client.rest_api import archive_url
from plantdb.client.rest_api import login_url
from plantdb.client.rest_api import logout_url
from plantdb.client.rest_api import make_api_request
from plantdb.client.rest_api import origin_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import refresh_url
from plantdb.client.rest_api import register_url
from plantdb.client.rest_api import request_api_token
from plantdb.client.rest_api import request_check_username
from plantdb.client.rest_api import request_login
from plantdb.client.rest_api import request_logout
from plantdb.client.rest_api import request_new_user
from plantdb.client.rest_api import request_scan_data
from plantdb.client.rest_api import request_scan_names_list
from plantdb.client.rest_api import request_scans_info
from plantdb.client.rest_api import request_token_refresh
from plantdb.client.rest_api import request_token_validation
from plantdb.client.rest_api import scan_config_url
from plantdb.client.rest_api import scan_file_url
from plantdb.client.rest_api import scan_image_url
from plantdb.client.rest_api import scan_reconstruction_url
from plantdb.client.rest_api import scan_url
from plantdb.client.rest_api import scans_url
from plantdb.client.rest_api import token_refresh_url
from plantdb.client.rest_api import token_validation_url


class TestRestApi(unittest.TestCase):

    def setUp(self):
        self.host = "localhost"
        self.port = 2020
        self.prefix = "/plantdb"
        self.attr_dict = {
            'port': self.port,
            'prefix': self.prefix,
        }

    def test_origin_url_basic(self):
        """Test basic origin_url functionality"""
        self.assertEqual(origin_url('example.com'), 'http://example.com')
        self.assertEqual(origin_url('example.com', 8080), 'http://example.com:8080')
        self.assertEqual(origin_url('https://example.com'), 'https://example.com')

    def test_origin_url_ssl_override(self):
        """Test SSL override functionality"""
        self.assertEqual(origin_url('http://example.com', ssl=True), 'https://example.com')
        self.assertEqual(origin_url('https://example.com', ssl=False), 'https://example.com')

    def test_plantdb_url(self):
        """Test plantdb_url functionality"""
        self.assertEqual(plantdb_url('localhost', 2020, prefix=''),
                         'http://localhost:2020')
        self.assertEqual(plantdb_url('localhost', 2020, prefix='', ssl=True),
                         'https://localhost:2020')
        self.assertEqual(plantdb_url('localhost', prefix='/plantdb'),
                         'http://localhost/plantdb/')

    def test_login_url(self):
        """Test login_url functionality"""
        self.assertEqual(login_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/login')

    def test_logout_url(self):
        """Test logout_url functionality"""
        self.assertEqual(logout_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/logout')

    def test_register_url(self):
        """Test register_url functionality"""
        self.assertEqual(register_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/register')

    def test_token_validation_url(self):
        """Test token_validation_url functionality"""
        self.assertEqual(token_validation_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/token/validation')

    def test_token_refresh_url(self):
        """Test token_refresh_url functionality"""
        self.assertEqual(token_refresh_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/token/refresh')

    def test_api_token_url(self):
        """Test api_token_url functionality"""
        self.assertEqual(api_token_url('localhost', **self.attr_dict),
                         'http://localhost:2020/plantdb/auth/token/create-api-token')

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
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '0', size='orig', as_base64=False, port=2020, prefix=''),
            'http://localhost:2020/assets/image/example/images/0?size=orig&as_base64=false'
        )
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '1', size='big', as_base64=False, port=2020, prefix=''),
            'http://localhost:2020/assets/image/example/images/1?size=big&as_base64=false'
        )
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '0', prefix='/plantdb'),
            'http://localhost/plantdb/assets/image/example/images/0?size=orig&as_base64=false'
        )
        self.assertEqual(
            scan_image_url('localhost', 'example', 'images', '1', size='big', prefix='/plantdb'),
            'http://localhost/plantdb/assets/image/example/images/1?size=big&as_base64=false')

    def test_refresh_url(self):
        """Test refresh_url functionality"""
        self.assertEqual(refresh_url(host='localhost', port=2020, prefix=''),
                         'http://localhost:2020/refresh')
        self.assertEqual(refresh_url('localhost', 'example', port=2020, prefix=''),
                         'http://localhost:2020/refresh?scan_id=example')
        self.assertEqual(refresh_url(host='localhost', prefix='/plantdb'),
                         'http://localhost/plantdb/refresh')
        self.assertEqual(refresh_url('localhost', 'example', prefix='/plantdb'),
                         'http://localhost/plantdb/refresh?scan_id=example')

    def test_archive_url(self):
        """Test archive_url functionality"""
        self.assertEqual(archive_url('localhost', 'real_plant', port=2020, prefix=''),
                         'http://localhost:2020/assets/archive/real_plant')
        self.assertEqual(archive_url('localhost', 'real_plant', prefix='/plantdb'),
                         'http://localhost/plantdb/assets/archive/real_plant')

    def test_scan_file_url(self):
        """Test scan_file_url functionality"""
        self.assertEqual(scan_file_url('localhost', 'dataset/file.txt', port=2020, prefix=''),
                         'http://localhost:2020/assets/files/dataset/file.txt')

    def test_scan_config_url(self):
        """Test scan_config_url functionality"""
        self.assertEqual(scan_config_url('localhost', 'real_plant', port=2020, prefix=''),
                         'http://localhost:2020/assets/files/real_plant/scan.toml')

    def test_scan_reconstruction_url(self):
        """Test scan_reconstruction_url functionality"""
        self.assertEqual(scan_reconstruction_url('localhost', 'real_plant', port=2020, prefix=''),
                         'http://localhost:2020/assets/files/real_plant/pipeline.toml')

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_login(self, mock_request):
        """Test request_login functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'refresh_token': 'test_refresh',
            'user': {'username': 'testuser'}
        }
        mock_request.return_value = mock_response

        result = request_login('localhost', 'testuser', 'password', port=2020)

        self.assertEqual(result['access_token'], 'test_token')
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_check_username(self, mock_request):
        """Test request_check_username functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {'exists': True}
        mock_request.return_value = mock_response

        result = request_check_username('localhost', 'testuser', port=2020)

        self.assertTrue(result)
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_logout(self, mock_request):
        """Test request_logout functionality"""
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {'message': 'Logged out'}
        mock_request.return_value = mock_response

        result = request_logout('localhost', port=2020, session_token='test_token')

        self.assertEqual(result, (True, 'Logged out'))
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_token_validation(self, mock_request):
        """Test request_token_validation functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {'user': {'username': 'testuser'}}
        mock_request.return_value = mock_response

        result = request_token_validation('localhost', port=2020, session_token='test_token')

        self.assertEqual(result['user']['username'], 'testuser')
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_token_refresh(self, mock_request):
        """Test request_token_refresh functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'access_token': 'new_access_token',
            'refresh_token': 'new_refresh_token'
        }
        mock_request.return_value = mock_response

        result = request_token_refresh('localhost', port=2020, refresh_token='refresh_token')

        self.assertEqual(result['access_token'], 'new_access_token')
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_api_token(self, mock_request):
        """Test request_api_token functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {'api_token': 'test_api_token'}
        mock_request.return_value = mock_response

        result = request_api_token('localhost', 3600, {'dataset': ('read',)}, port=2020, session_token='test_token')

        self.assertEqual(result['api_token'], 'test_api_token')
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_new_user(self, mock_request):
        """Test request_new_user functionality"""
        mock_response = MagicMock()
        mock_response.ok = True
        mock_request.return_value = mock_response

        result = request_new_user('localhost', 'testuser', 'password', 'Test User', port=2020,
                                  session_token='test_token')

        self.assertTrue(result)
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_scan_names_list(self, mock_request):
        """Test request_scan_names_list functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = ['scan1', 'scan2']
        mock_request.return_value = mock_response

        result = request_scan_names_list('localhost', port=2020, session_token='test_token')

        self.assertEqual(result, ['scan1', 'scan2'])
        mock_request.assert_called_once()

    @patch('plantdb.client.rest_api.request_scan_names_list')
    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_scans_info(self, mock_request, mock_names):
        """Test request_scans_info functionality"""
        mock_names.return_value = ['scan1', 'scan2']
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 'scan1'}
        mock_request.return_value = mock_response

        result = request_scans_info('localhost', port=2020, session_token='test_token')

        self.assertEqual(len(result), 2)
        mock_request.assert_called()

    @patch('plantdb.client.rest_api.make_api_request')
    def test_request_scan_data(self, mock_request):
        """Test request_scan_data functionality"""
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 'scan1', 'name': 'test_scan'}
        mock_request.return_value = mock_response

        result = request_scan_data('localhost', 'scan1', port=2020, session_token='test_token')

        self.assertEqual(result['name'], 'test_scan')
        mock_request.assert_called_once()

    def test_make_api_request_get(self):
        """Test make_api_request with GET method"""
        with patch('plantdb.client.rest_api.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = {'test': 'data'}
            mock_get.return_value = mock_response

            result = make_api_request('http://localhost', method='GET')

            self.assertEqual(result.json(), {'test': 'data'})

    def test_make_api_request_post(self):
        """Test make_api_request with POST method"""
        with patch('plantdb.client.rest_api.requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = {'test': 'data'}
            mock_post.return_value = mock_response

            result = make_api_request('http://localhost', method='POST', json_data={'key': 'value'})

            self.assertEqual(result.json(), {'test': 'data'})

    def test_make_api_request_invalid_method(self):
        """Test make_api_request with invalid method"""
        with self.assertRaises(ValueError):
            make_api_request('http://localhost', method='INVALID')

    def test_centralized_variables(self):
        """Test that centralized variables are properly initialized"""
        # Test default values
        self.assertEqual(os.getenv('PLANTDB_HOST', "localhost"), "localhost")
        self.assertEqual(os.getenv('PLANTDB_PORT', ''), '')
        self.assertEqual(os.getenv('PLANTDB_PREFIX', None), None)


if __name__ == '__main__':
    unittest.main()
