#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

from ddr_compress import dbi as pdbi
import paperdata_dbi
import add_files


### Script to load md5sums into paperdata database
### Loads md5sums

### Author: Immanuel Washington
### Date: 5-06-15

def md5_db():
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).filter(dbi.File.md5sum==None).all()
	s.close()
	for FILE in FILEs:
		md5 = add_files.calc_md5sum(FILE.host, FILE.path, FILE.filename)
		dbi.set_file_md5(FILE.full_path, md5)

	return None

if __name__ == '__main__':
	md5_db()
