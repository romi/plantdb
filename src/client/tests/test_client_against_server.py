import unittest
import time

from plantdb.server.test_rest_api import TestRestApiServer
from plantdb.client import rest_api as client

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
        cls.kw = dict(host=cls.server.host, port=cls.server.port, prefix=cls.server.prefix, ssl=cls.server.ssl)

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

        Parameters
        ----------
        self : obj
            The instance of the test class.
        client : obj
            A client object used to interact with the scanning service.
        kw : dict
            Keyword arguments required for the client operations.
        """
        names = client.list_scan_names(**self.kw)
        self.assertIsInstance(names, list)
        self.assertGreater(len(names), 0)
        scan_id = names[0]

        info = client.get_scan_data(scan_id, **self.kw)
        self.assertEqual(info.get("id"), scan_id)

        scans_info = client.get_scans_info(**self.kw)
        self.assertIsInstance(scans_info, list)

    def test_preview_and_images_helpers(self):
        """
        Test the preview and image-related helpers.

        This method tests the functionality of generating URLs for scan preview images,
        listing task image URIs, and downloading images from a specified task.
        It ensures that URL building is correct and requests complete successfully
        with status codes 200 or 404 depending on the dataset.
        """
        names = client.list_scan_names(**self.kw)
        scan_id = names[0]
        print(f"Selected scan ID: {scan_id}")
        # just ensure URL builds and request completes (200 or 404 acceptable depending on dataset)
        url = client.scan_preview_image_url(scan_id, size="thumb", **self.kw)
        print(f"URL: {url}")
        self.assertIn("/image/", url)

        # list task images
        uris = client.list_task_images_uri(scan_id, task_name='images', size='orig', **self.kw)
        self.assertIsInstance(uris, list)
        # download images if any
        imgs = client.get_images_from_task(scan_id, task_name='images', size='orig', **self.kw)
        self.assertIsInstance(imgs, list)

    def test_refresh_and_archive(self):
        """
        Tests the refresh and archive functionality of the client.

        This test method performs several actions to verify the correct behavior of the client's refresh and archive features. It first calls the `refresh` method and checks that the response is a dictionary containing a "message" key. Then, it retrieves a list of scan names and selects the first one as the target for archiving. The `download_scan_archive` method is called with this scan ID and no specified output directory, expecting to receive a tuple consisting of a BytesIO object and a message string.

        Parameters
        ----------
        self : TestCase
            The test case instance.
        """
        res = client.refresh(**self.kw)
        self.assertIsInstance(res, dict)
        self.assertIn("message", res)

        names = client.list_scan_names(**self.kw)
        scan_id = names[0]
        # Download archive to temp dir
        res = client.download_scan_archive(scan_id, out_dir=None, **self.kw)
        # when out_dir is None, a BytesIO and message tuple is expected
        import io
        self.assertIsInstance(res, tuple)
        self.assertIsInstance(res[0], io.BytesIO)
        self.assertIsInstance(res[1], str)


if __name__ == '__main__':
    unittest.main()
