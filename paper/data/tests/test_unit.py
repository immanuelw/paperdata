'''
paper.data.tests.test_unit

runs unit on paperdata modules and scripts

author | Immanuel Washington

Classes
-------
TestDBI | test database interface
TestUVData | tests uv data module
TestFileData | tests file data module
'''
from __future__ import print_function
import unittest
from paper.data import dbi as pdbi, uv_data, file_data

class TestDBI(unittest.TestCase):
    def setUp(self):
        self.dbi = pdbi.DataBaseInterface()
        self.obs_table = pdbi.Observation
        self.file_table = pdbi.File
        self.log_table = pdbi.Log
        #self.feed_table = pdbi.Feed
        #self.sess = dbi.Session

    def tearDown(self):
        #self.sess.close()
        pass

    def test_get_entry(self):
        #obsnum = self.dbi.get_entry(self.sess, 'Observation', VALUE).obsnum
        #self.assertEqual(obsnum, OBSNUM, msg='Observation not in database')

        #source = self.dbi.get_entry(self.sess, 'File', VALUE).source
        #self.assertEqual(source, SOURCE, msg='File not in database')
        pass

    def test_query(self):
        #obsnum = self.sess.query(self.obs_table)\
        #                  .filter(self.obs_table.julian_date == JULIANDATE)\
        #                  .filter(self.obs_table.polarization == POLARIZATION)\
        #                  .first().obsnum
        #self.assertEqual(obsnum, OBSNUM, msg='Observation entry not found)

        #file_query = self.sess.query(self.file_table)\
        #                      .filter(self.file_table.host == HOST)\
        #                      .filter(self.file_table.base_path == BASEPATH)\
        #                      .order_by(file_table.filename)
        #source = file_query.first().source
        #self.assertEqual(source, SOURCE), msg='File entry not found)
        pass

class TestUVData(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

class TestFileData(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
