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
import os
from decimal import Decimal
import unittest
import aipy as A
import paper as ppdata
from paper.data import dbi as pdbi, uv_data, file_data

class TestDBI(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDBI, self).__init__(*args, **kwargs)
        self.source = os.path.join('folio.sas.upenn.edu:', ppdata.root_dir, 'data/test', 'zen.2456617.22257.yx.uvcRRE')
        self.host = 'folio.sas.upenn.edu'
        self.base_path = os.path.join(ppdata.root_dir, 'data/test')
        self.jd = 2456600
        self.pol = 'xx'
        self.obsnum = 21480810617
        self.dbi = pdbi.DataBaseInterface()
        self.obs_table = pdbi.Observation
        self.file_table = pdbi.File
        self.log_table = pdbi.Log

    def test_query(self):
        with self.dbi.session_scope() as s:
            obs_query = s.query(self.obs_table)\
                                 .filter(self.obs_table.julian_date == self.jd)\
                                 .filter(self.obs_table.polarization == self.pol)
            obsnum = obs_query.first().obsnum
            self.assertEqual(obsnum, self.obsnum, msg='Observation entry not found')

            file_query = s.query(self.file_table)\
                                  .filter(self.file_table.host == self.host)\
                                  .filter(self.file_table.base_path == self.base_path)\
                                  .order_by(self.file_table.filename)
            source = file_query.first().source
            self.assertEqual(source, self.source, msg='File entry not found')

            joined_query = s.query(self.file_table)\
                                    .join(self.obs_table)\
                                    .filter(self.obs_table.julian_date == self.jd)\
                                    .filter(self.obs_table.polarization == self.pol)
                                    .filter(self.file_table.host == self.host)\
                                    .filter(self.file_table.base_path == self.base_path)\
                                    .order_by(self.file_table.filename)
            obsnum = joined_query.first().obsnum
            self.assertEqual(obsnum, self.obsnum, msg='Joined observation entry not found')

class TestUVData(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestUVData, self).__init__(*args, **kwargs)
        self.dbi = pdbi.DataBaseInterface()
        self.uv_file = os.path.join(ppdata.root_dir, 'data/test', 'zen.2456617.22257.yx.uvcRRE')
        self.npz_file = os.path.join(ppdata.root_dir, 'data/test', 'zen.2455906.53332.uvcRE.npz')

    def test_jd(self):
        j_date = uv_data.five_round(2455903.1667113231)
        self.assertEqual(j_date, 2455903.16671, msg='Issue with julian date rounding')

        obsnum = uv_data.jdpol_to_obsnum(2456600, 'xx', 0.00696)
        self.assertEqual(obsnum, 21480810617, msg='Issue with obsnum generation')

        jd_info = uv_data.date_info(2456604.16671)
        self.assertSequenceEqual(jd_info, (128, 2456604, 20.8), msg='Issue with pulling relevant info from julian date')

    def test_edge(self):
        edge_1 = uv_data.is_edge(None, None)
        edge_2 = uv_data.is_edge(None, object)
        edge_3 = uv_data.is_edge(object, object)

        self.assertIsNone(edge_1)
        self.assertTrue(edge_2)
        self.assertFalse(edge_3)

    def test_times(self):
        uv = A.miriad.UV(self.uv_file)

        times = (2456617.17386, 2456617.18032, 0.0005, 0.00696)
        c_times = uv_data.calc_times(uv)
        self.assertSequenceEqual(c_times, times, msg='Calculated times differ from expected')

    def test_npz(self):
        c_data = uv_data.calc_npz_data(self.dbi, self.npz_file)
        npz_data = (Decimal('2455906.53332'), Decimal('2455906.54015'), Decimal('0.00012'),
                    2455906.53332, 'all', Decimal('0.00696'), 17185743685)#L?
        self.assertSequenceEqual(c_data, npz_data, msg='Calculated information differ from expected')

    def test_uv(self):
        c_data = uv_data.calc_uv_data('folio', self.uv_file)
        UV_data = (2456617.17386, 2456617.18032, 0.0005, 2456617.18069, 'xx', 0.00696, 21480813086)
        self.assertSequenceEqual(c_data, UV_data, msg='Calculated information differ from expected')

class TestFileData(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFileData, self).__init__(*args, **kwargs)
        self.uv_file = os.path.join(ppdata.root_dir, 'data/test', 'zen.2456617.22257.yx.uvcRRE')

    def test_names(self):
        c_names = file_data.file_names(self.uv_file)
        names = ('/home/immwa/test_data', 'zen.2456617.22257.yx.uvcRRE', 'uvcRRE')
        self.assertSequenceEqual(c_names, names, msg='Names seperated incorrectly')

    def test_filesize(self):
        byte_sz = file_data.byte_size(self.uv_file)
        BYSZ = 215132692
        self.assertEqual(byte_sz, BYSZ, msg='Byte size is different')

        hu_sz = file_data.human_size(1048576)
        HUSZ = 1.0
        self.assertEqual(hu_sz, HUSZ, msg='Human readable size is different')

        mb_sz = file_data.calc_size('folio', self.uv_file)
        MBSZ = 205.2
        self.assertEqual(mb_sz, MBSZ, msg='MB size is different')

    def test_md5(self):
        c_md5 = file_data.get_md5sum(self.uv_file)
        md5 = '7d5ac942dd37c4ddfb99728359e42331'
        self.assertEqual(c_md5, md5, msg='md5sum generated is wrong')

        nc_md5 = file_data.calc_md5sum('folio', self.uv_file)
        self.assertEqual(nc_md5, md5, msg='md5sum generated is wrong')

    def test_parse_sources(self):
        source_paths_str = os.path.join(ppdata.root_dir, 'data/test', 'zen.*.uv*')
        source_paths = file_data.parse_sources('folio', source_paths_str)
        file_paths = (os.path.join(ppdata.root_dir, 'data/test', 'zen.2455906.53332.uvcRE.npz'),
                      os.path.join(ppdata.root_dir, 'data/test', 'zen.2456617.22257.yx.uvcRRE'))
        self.assertSequenceEqual(source_paths, file_paths, msg='list of paths differ from expected')

if __name__ == '__main__':
    unittest.main()
