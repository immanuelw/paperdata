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

def md5_db():
	'''
	updates md5sums for all files without in database
	'''
	data_dbi = pdbi.DataBaseInterface()
	s = data_dbi.Session()
	table = getattr(pdbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
	for FILE in FILEs:
		md5 = file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'path'), getattr(FILE, 'filename'))
		timestamp = int(time.time())
		data_dbi.set_entry(s, FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'path'), getattr(FILE, 'filename'))
		data_dbi.set_entry(s, FILE, 'timestamp', timestamp)
		log_data = {'action':'update md5sum',
					'table':'file',
					'obsnum':getattr(FILE, 'full_path'),
					'timestamp':timestamp}

		data_dbi.add_entry(s, 'log', log_data)
	s.close()

	return None

def md5_distiller():
	'''
	updates md5sums for all files without in database
	'''
	dbi = ddbi.DataBaseInterface()
	s = dbi.Session()
	table = getattr(ddbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
	for FILE in FILEs:
		full_path = getattr(FILE, 'path')
		setattr(FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), os.path.dirname(full_path), os.path.basename(full_path)))
		s.add(FILE)
		s.commit()
	s.close()

	return None

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Input argument -- [paper/paperdistiller] to select which database to update md5sums')
	elif sys.argv[1] == 'paper':
		md5_db()
	elif sys.argv[1] == 'paperdistiller':
		md5_distiller()
	else:
		print('Unallowed argument(s)')
