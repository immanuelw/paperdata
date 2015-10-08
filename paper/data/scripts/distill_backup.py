#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import sys
import time
import os
import json
import decimal
import paper as ppdata
import ddr_compress.dbi as ddbi

### Script to Backup paper database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

def to_dict(ser_data):
	'''
	creates a dict of database object's attributes

	Parameters
	----------
	ser_data | object: database object

	Returns
	-------
	dict: database object's attributes
	'''
	json_dict = ser_data.__dict__
	try:
		del json_dict['_sa_instance_state']
	except(KeyError):
		return None

	return json_dict

def json_data(dbo, dump_objects):
	'''
	dumps list of objects into a json file

	Parameters
	----------
	dbo | str: filename
	dump_objects | list[object]: database objects query
	'''
	with open(dbo, 'w') as f:
		data = [to_dict(ser_data) for ser_data in dump_objects.all()]
		json.dump(data, f, sort_keys=True, indent=1, default=ppdata.decimal_default)

def paperbackup(dbi):
	'''
	backups database by loading into json files, named by timestamp

	Parameters
	----------
	dbi | object: database interface object
	'''
	timestamp = int(time.time())
	backup_dir = os.path.join('/data4/paper/paperdistiller_backup', str(timestamp))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	tables = ('Observation', 'File', 'Log')
	table_sorts = {'Observation': {'first': 'julian_date', 'second': 'pol'},
					'File': {'first': 'obsnum', 'second': 'filename'},
					'Log': {'first': 'obsnum', 'second': 'timestamp'}}
	s = dbi.Session()
	print(timestamp)
	for table in tables:
		db_file = '{table}_{timestamp}.json'.format(table=table.lower(), timestamp=timestamp)
		dbo = os.path.join(backup_dir, db_file)
		print(db_file)
			DB_table = getattr(ddbi, table)
			DB_dump = s.query(DB_table).order_by(getattr(DB_table, table_sorts[table]['first']).asc(),
											getattr(DB_table, table_sorts[table]['second']).asc())
		json_data(dbo, DB_dump)
		print('Table data backup saved')
	s.close()

if __name__ == '__main__':
	dbi = ddbi.DataBaseInterface()
	paperbackup(dbi)
