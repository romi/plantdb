import os
import unittest
import tempfile

import imageio
import open3d
import numpy as  np

from romidata import io
from romidata.testing import DBTestCase

class TestIO(DBTestCase):
    """TODO : tests for all other IO...
    """
    def test_write_image(self):
        fs = self.get_test_fileset()
        img = np.zeros((1,1), dtype=np.uint8)

        f = fs.create_file("test_image_png")
        img_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_image_png.png")
        io.write_image(f, img, "png")
        assert(os.path.exists(img_path))
        img_read = imageio.imread(img_path)
        assert(img_read[0,0] == 0)

        f = fs.create_file("test_image_jpg")
        img_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_image_jpg.jpg")
        io.write_image(f, img, "jpg")
        assert(os.path.exists(img_path))
        img_read = imageio.imread(img_path)
        assert(img_read[0,0] == 0)

    def test_read_image(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("image")
        img = io.read_image(file)

        assert(img[0,0] == 255)
        assert(img[0,1] == 0)
        assert(img[1,0] == 0)
        assert(img[1,1] == 255)





if __name__ == "__main__":
    unittest.main()
