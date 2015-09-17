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
	data_dbi = pdbi.DataBaseInterface()
	s = data_dbi.Session()
	table = getattr(pdbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
	s.close()
	for FILE in FILEs:
		md5 = file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'path'), getattr(FILE, 'filename'))
		timestamp = int(time.time())
		data_dbi.set_entry(FILE, 'md5sum', md5)
		data_dbi.set_entry(FILE, 'timestamp', timestamp)
		action = 'update md5sum'
		table = 'file'
		identifier = getattr(FILE, 'full_path')
		log_data = {'action':action,
					'table':table,
					'obsnum':identifier,
					'timestamp':timestamp}

		data_dbi.add_log(log_data)

	return None

def md5_distiller():
	dbi = ddbi.DataBaseInterface()
	s = dbi.Session()
	table = getattr(ddbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
	s.close()
	for FILE in FILEs:
		full_path = getattr(FILE, 'path')
		path = os.path.dirname(full_path)
		filename = os.path.basename(full_path)
		md5 = file_data.calc_md5sum(getattr(FILE, 'host'), path, filename)
		setattr(FILE, 'md5sum', md5)
		s = dbi.Session()
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
