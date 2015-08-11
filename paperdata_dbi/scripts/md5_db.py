#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import time
from ddr_compress.dbi import DataBaseInterface, File, Observation
import paperdata_dbi as pdbi
import add_files

### Script to load md5sums into paperdata database
### Loads md5sums

### Author: Immanuel Washington
### Date: 5-06-15

def md5_db():
	data_dbi = pdbi.DataBaseInterface()
	s = data_dbi.Session()
	FILEs = s.query(data_dbi.File).filter(data_dbi.File.md5sum==None).all()
	s.close()
	for FILE in FILEs:
		md5 = add_files.calc_md5sum(FILE.host, FILE.path, FILE.filename)
		timestamp = int(time.time())
		data_dbi.set_file_md5(FILE.full_path, md5)
		data_dbi.set_file_time(FILE.full_path, timestamp)

	return None

def md5_distiller():
	dbi = DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(pdbi.File).filter(pdbi.File.md5sum==None).all()
	s.close()
	for FILE in FILEs:
		full_path = FILE.path
		path = os.path.dirname(full_path)
		filename = os.path.basename(full_path)
		md5 = add_files.calc_md5sum(FILE.host, path, filename)
		FILE.md5sum = md5
		s = dbi.Session()
		s.add(FILE)
		s.commit()
		s.close()

	return None

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Input argument -- [paperdata/paperdistiller] to select which database to update md5sums')
	elif sys.argv[1] == 'paperdata':
		md5_db()
	elif sys.argv[1] == 'paperdistiller':
		md5_distiller()
	else:
		print('Unallowed argument(s)')
