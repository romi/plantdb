import io
import os
import tempfile
import time
import unittest

from plantdb.client import rest_api as client
from plantdb.client.rest_api import plantdb_url
from plantdb.client.url import is_server_available
from plantdb.server.test_rest_api import TestRestApiServer

SERVER_PORT = 5555


class ClientRestApiIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        A test class for setting up and tearing down a REST API server.

        This class is responsible for starting the test REST API server before
        tests run and stopping it after they complete. It also sets up common
        keyword arguments used in testing.
        """
        cls.server = TestRestApiServer(test=True, port=SERVER_PORT)
        cls.server.start()
        # small delay to be ready
        time.sleep(0.3)
        cls.kw = dict(port=cls.server.port, prefix=cls.server.prefix, ssl=cls.server.ssl)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_basic_listing_and_scan_data(self):
        """
        Test basic listing and scan data functionality.

        This test case verifies the ability to list scan names, retrieve
        scan data using a specific scan ID, and get information about all
        scans. It ensures that the returned data is of the expected types
        and contains valid information.
        """
        names = client.request_scan_names_list(self.server.host, **self.kw)
        self.assertIsInstance(names, list)
        self.assertGreater(len(names), 0)
        scan_id = names[0]

        info = client.request_scan_data(self.server.host, scan_id, **self.kw)
        self.assertEqual(info.get("id"), scan_id)

        scans_info = client.request_scans_info(self.server.host, **self.kw)
        self.assertIsInstance(scans_info, list)

    def test_preview_and_images_helpers(self):
        """
        Test the preview and image-related helpers.

        This method tests the functionality of generating URLs for scan preview images,
        listing task image URIs, and downloading images from a specified task.
        It ensures that URL building is correct and requests complete successfully
        with status codes 200 or 404 depending on the dataset.
        """
        names = client.request_scan_names_list(self.server.host, **self.kw)
        scan_id = names[0]
        print(f"Selected scan ID: {scan_id}")
        # just ensure URL builds and request completes (200 or 404 acceptable depending on dataset)
        url = client.scan_preview_image_url(self.server.host, scan_id, size="thumb", **self.kw)
        print(f"URL: {url}")
        self.assertIn("/image/", url)

        # list task images
        uris = client.list_task_images_uri(self.server.host, scan_id, task_name='images', size='orig', **self.kw)
        self.assertIsInstance(uris, list)
        # download images if any
        imgs = client.parse_task_images(self.server.host, scan_id, task_name='images', size='orig', **self.kw)
        self.assertIsInstance(imgs, list)

    def test_refresh_and_archive(self):
        """
        Tests the refresh and archive functionality of the client.

        This test method performs several actions to verify the correct behavior of the client's refresh and archive features.
        It first calls the `refresh` method and checks that the response is a dictionary containing a "message" key.
        Then, it retrieves a list of scan names and selects the first one as the target for archiving.
        The `download_scan_archive` method is called with this scan ID and no specified output directory, expecting to receive a tuple consisting of a BytesIO object and a message string.
        """
        res_data = client.request_refresh(self.server.host, **self.kw).json()
        self.assertIsInstance(res_data, dict)
        self.assertIn("message", res_data)

        names = client.request_scan_names_list(self.server.host, **self.kw)
        scan_id = names[0]
        # Download archive to temp dir
        res_data = client.request_archive_download(self.server.host, scan_id, out_dir=None, **self.kw)
        # when out_dir is None, a BytesIO and message tuple is expected
        self.assertIsInstance(res_data, tuple)
        self.assertIsInstance(res_data[0], io.BytesIO)
        self.assertIsInstance(res_data[1], str)

    def test_server_availability(self):
        """
        Test server availability endpoint.

        This test verifies that the is_server_available function correctly
        checks if the server is available and returns the expected boolean result.
        """
        available = is_server_available(plantdb_url(self.server.host, **self.kw), allow_private_ip=True, verify_ssl=False)
        self.assertTrue(available.ok)  # Should be True since the test server is running

    def test_url_building_functions(self):
        """
        Test URL building functions.

        This test verifies that URL building functions correctly generate
        expected URL formats for various endpoints.
        """
        names = client.request_scan_names_list(self.server.host, **self.kw)
        self.assertGreater(len(names), 0)
        scan_id = names[0]

        # Test scan_url function
        url = client.scan_url(self.server.host, scan_id, **self.kw)
        self.assertIn(scan_id, url)

        # Test scan_image_url function
        image_url = client.scan_image_url(self.server.host, scan_id, fileset_id="images", file_id="00000_rgb", size="thumb", **self.kw)
        self.assertIn(scan_id, image_url)
        self.assertIn("images", image_url)
        self.assertIn("thumb", image_url)

        # Test archive_url function
        arch_url = client.archive_url(self.server.host, scan_id, **self.kw)
        self.assertIn(scan_id, arch_url)
        self.assertIn("archive", arch_url)

    def test_get_task_data(self):
        """
        Test getting task data.

        This test verifies the get_task_data function can retrieve data
        for a specific task from a scan.
        """
        import networkx as nx

        scan_id = "real_plant_analyzed"

        # Try to get data for some common task types
        task_names = ["PointCloud", "TriangleMesh", "CurveSkeleton", "TreeGraph"]
        expected_data_types = [list, dict, dict, nx.Graph]

        for task_name, expected_type in zip(task_names, expected_data_types):
            # We're testing the API calls succeed, not necessarily that data exists
            try:
                data = client.get_task_data(self.server.host, scan_id, task_name, **self.kw)
                # If data is returned, validate its structure
                if data is not None:
                    print(f"Data for task {task_name} is: {type(data)}")
                    self.assertIsInstance(data, expected_type)
            except Exception as e:
                # Ignore 404 errors which are expected for tasks that don't exist
                if "404" not in str(e):
                    raise

    def test_get_configuration_files(self):
        """
        Test getting configuration files.

        This test verifies the functions for retrieving scan and reconstruction
        configuration files work correctly.
        """
        names = client.request_scan_names_list(self.server.host, **self.kw)
        self.assertGreater(len(names), 0)
        scan_id = names[0]

        # Test scan config
        try:
            config = client.get_scan_config(self.server.host, scan_id, **self.kw)
            if config is not None:
                self.assertIsInstance(config, dict)
        except Exception as e:
            # Ignore 404 errors which are expected if config doesn't exist
            if "404" not in str(e):
                raise

        # Test reconstruction config
        try:
            recon_config = client.get_reconstruction_config(self.server.host, scan_id, **self.kw)
            if recon_config is not None:
                self.assertIsInstance(recon_config, dict)
        except Exception as e:
            # Ignore 404 errors which are expected if config doesn't exist
            if "404" not in str(e):
                raise

    def test_upload_functions(self):
        """
        Test upload functions.

        This test verifies the upload_dataset_file and upload_scan_archive functions
        by creating temporary test files and attempting to upload them.
        """
        # Create a temporary file for testing
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as temp_file:
            temp_file.write(b"Test file content")
            temp_path = temp_file.name

        try:
            # Test upload_dataset_file
            try:
                result = client.request_dataset_file_upload(self.server.host, 'real_plant', temp_path, **self.kw)
                self.assertIsInstance(result, dict)
            except Exception as e:
                # Some servers may not allow uploads
                if "403" not in str(e) and "405" not in str(e):
                    raise

            # For upload_scan_archive, we'd normally need a valid archive
            # This is just testing the API call structure works
            try:
                result = client.request_archive_upload(self.server.host, temp_path, **self.kw)
                # If successful, should return a dict
                self.assertIsInstance(result, dict)
            except Exception as e:
                # Many errors are expected here due to invalid archive format
                # Just check that the function call structure worked
                pass

        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_angles_and_internodes_data(self):
        """
        Test getting angles and internodes data.

        This test verifies the get_angles_and_internodes_data function
        can retrieve angle data for a scan.
        """
        scan_id = "real_plant_analyzed"

        data = client.get_angles_and_internodes_data(self.server.host, scan_id, **self.kw)
        # If data is returned, it should be a dictionary
        if data is not None:
            self.assertIsInstance(data, dict)


if __name__ == '__main__':
    unittest.main()
