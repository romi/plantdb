import json
import os
import tempfile
import time
import unittest
from io import BytesIO

import requests

from plantdb.client.plantdb_client import PlantDBClient
from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class RestApiServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start a test server with the built-in test dataset
        cls.server = TestRestApiServer(db_path=_mkdtemp_romidb(), test=True)
        cls.server.start()
        # Wait briefly for the server to start
        for _ in range(10):
            try:
                r = requests.get(cls.server.get_base_url() + "/health")
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)

        cls.client = PlantDBClient(cls.server.get_base_url())

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_home(self):
        r = requests.get(self.server.get_base_url() + "/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("PlantDB", r.json().get("name", ""))

    def test_login_logout_endpoints(self):
        # First attempt with unknown user should fail
        r = requests.post(self.server.get_base_url() + '/login',
                          json={'username': 'anonymous', 'password': 'AlanMoore'})
        self.assertEqual(r.status_code, 401)
        # The first attempt without a login should fail
        r = requests.post(self.server.get_base_url() + '/logout')
        self.assertEqual(r.status_code, 401)

        # Login to get authentication token
        r = requests.post(self.server.get_base_url() + '/login', json={'username': 'admin', 'password': 'admin'})
        self.assertEqual(r.status_code, 200)
        jwt_token = r.json()['access_token']
        r = requests.post(self.server.get_base_url() + '/logout', headers={'Authorization': 'Bearer ' + jwt_token})
        self.assertEqual(r.status_code, 200)

    def test_scans_list_and_table(self):
        # /scans
        r = requests.get(self.server.get_base_url() + "/scans")
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertIsInstance(scans, list)
        self.assertGreaterEqual(len(scans), 1)

        from pathlib import Path
        print(list(Path(self.server.db_path).iterdir()))
        scan_path = Path(self.server.db_path) / 'real_plant_test'
        if scan_path.exists():
            print(list((scan_path).iterdir()))
        else:
            print(f"'{scan_path}' does not exist")

        # /scans_info table
        r = requests.get(self.server.get_base_url() + "/scans_info")
        self.assertEqual(r.status_code, 200)
        table = r.json()
        self.assertIsInstance(table, list)
        self.assertGreaterEqual(len(table), 1)

    def test_scan_get(self):
        # Use first scan
        scans = self.client.list_scans()
        scan_id = scans[0]
        r = requests.get(self.server.get_base_url() + f"/scan/{scan_id}/metadata")
        self.assertEqual(r.status_code, 200)
        info = r.json()
        self.assertIn("metadata", info)
        self.assertIn("owner", info['metadata'])

    def test_image_endpoint(self):
        scan_id = 'real_plant_analyzed'
        r = requests.get(self.server.get_base_url() + f'/image/{scan_id}/images/00000_rgb', stream=True)
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)

    def test_pointcloud_endpoint(self):
        scan_id = 'real_plant_analyzed'
        r = requests.get(
            self.server.get_base_url() + f'/pointcloud/{scan_id}/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud',
            stream=True)
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)

    def test_mesh_endpoint(self):
        scan_id = 'real_plant_analyzed'
        r = requests.get(
            self.server.get_base_url() + f'/mesh/{scan_id}/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh',
            stream=True)
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)

    def test_skeleton_endpoint(self):
        scan_id = 'real_plant_analyzed'
        r = requests.get(self.server.get_base_url() + f'/skeleton/{scan_id}', stream=True)
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)
        self.assertIsNotNone(json.loads(r.content))

    def test_sequence_endpoint(self):
        scan_id = 'real_plant_analyzed'
        r = requests.get(self.server.get_base_url() + f'/sequence/{scan_id}', stream=True)
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)
        self.assertIsNotNone(json.loads(r.content.decode('utf-8')))

    def test_archive_endpoint(self):
        scan_id = 'real_plant'
        # Test GET without authentication
        r = requests.get(self.server.get_base_url() + f'/archive/{scan_id}', stream=True)
        self.assertEqual(r.status_code, 200)

        # Test GET with authentication
        r = requests.post(self.server.get_base_url() + '/login', json={'username': 'admin', 'password': 'admin'})
        jwt_token = r.json()['access_token']
        r = requests.get(self.server.get_base_url() + f'/archive/{scan_id}', stream=True,
                         headers={'Authorization': 'Bearer ' + jwt_token})
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(r.content)
        self.assertIsNotNone(BytesIO(r.content))

        # Create a unique temporary file name with .zip extension
        temp_zip_handle, temp_zip_path = tempfile.mkstemp(suffix='.zip')
        os.close(temp_zip_handle)  # Close the file handle immediately

        # Check if the request was successful
        if r.status_code == 200:
            # Open the destination file in write-binary mode
            with open(temp_zip_path, 'wb') as f:
                f.write(r.content)
        else:
            print(f"Error downloading file: {r.json().get('error', 'Unknown error')}")

        # After downloading - Test integrity of ZIP file
        from zipfile import ZipFile
        try:
            with ZipFile(temp_zip_path, 'r') as zip_ref:
                # Test if the ZIP file is valid
                test_result = zip_ref.testzip()
                self.assertIsNone(test_result, f"First bad file in ZIP: {test_result}")
        except Exception as e:
            print(f"Error verifying ZIP file: {e}")

        # Test POST with proper authentication
        new_dataset = f'{scan_id}_test'
        with open(temp_zip_path, 'rb') as zip_f:
            files = {
                'zip_file': (f'{new_dataset}.zip', zip_f, 'application/zip')
            }

            # Test POST without authentication - should fail with 401
            r = requests.post(self.server.get_base_url() + f'/archive/{new_dataset}', files=files)
            self.assertEqual(r.status_code, 401)

            # Ensure the file pointer is at the beginning before the second request
            zip_f.seek(0)

            # Test POST with proper authentication
            r = requests.post(self.server.get_base_url() + f'/archive/{new_dataset}',
                              files=files,
                              headers={'Authorization': 'Bearer ' + jwt_token})
            self.assertEqual(r.status_code, 200)

        # Clean up the temporary file
        os.unlink(temp_zip_path)

        r = requests.get(self.server.get_base_url() + "/scans")
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertIsInstance(scans, list)
        self.assertIn(new_dataset, scans)

        # Log out
        r = requests.post(self.server.get_base_url() + '/logout', headers={'Authorization': 'Bearer ' + jwt_token})

    def test_refresh(self):
        r = requests.get(self.server.get_base_url() + "/refresh")
        self.assertIn(r.status_code, (200, 429))
        self.assertIn("message", r.json())


if __name__ == '__main__':
    unittest.main()
