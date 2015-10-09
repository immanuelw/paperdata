#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paper tables

from paper.data import file_data
from paper.distiller import dbi as ddbi

### Script to load md5sums into paper database
### Loads md5sums

### Author: Immanuel Washington
### Date: 5-06-15

def update_md5(dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	dbi | object: distiller database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(ddbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
		for FILE in FILEs:
			dbi.set_entry(s, FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'filename')))

if __name__ == '__main__':
	dbi = ddbi.DataBaseInterface()
	update_md5(dbi)
