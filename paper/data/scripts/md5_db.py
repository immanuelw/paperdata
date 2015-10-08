#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paper tables

import time
import ddr_compress.dbi as ddbi
from paper.data import dbi as pdbi, file_data

### Script to load md5sums into paper database
### Loads md5sums

### Author: Immanuel Washington
### Date: 5-06-15

def md5_db(data_dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	data_dbi | object: data database interface object
	'''
	with data_dbi.session_scope() as s:
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
		for FILE in FILEs:
			full_path = getattr(FILE, 'full_path')
			timestamp = int(time.time())
			data_dbi.set_entry(s, FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), full_path))
			data_dbi.set_entry(s, FILE, 'timestamp', timestamp)
			log_data = {'action': 'update md5sum',
						'table': 'File',
						'identifier': full_path,
						'timestamp': timestamp}

			data_dbi.add_entry(s, 'Log', log_data)

def md5_distiller(dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	dbi | object: distiller database interface object
	'''
	s = dbi.Session()
	table = getattr(ddbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
	for FILE in FILEs:
		setattr(FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'filename')))
		s.add(FILE)
		s.commit()
	s.close()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Input argument -- [paperdata/paperdistiller] to select which database to update md5sums')
	elif sys.argv[1] == 'paperdata':
		data_dbi = pdbi.DataBaseInterface()
		md5_db(data_dbi)
	elif sys.argv[1] == 'paperdistiller':
		dbi = ddbi.DataBaseInterface()
		md5_distiller(dbi)
	else:
		print('Unallowed argument(s)')
