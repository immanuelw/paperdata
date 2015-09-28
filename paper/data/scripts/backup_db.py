#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
from __future__ import print_function
import sys
import time
import os
import json
import decimal
from paper.data import dbi as pdbi

### Script to Backup paper database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

def decimal_default(obj):
	'''
	fixes decimal issue with json module
	'''
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def json_data(dbo, dump_objects):
	'''
	dumps list of objects into a json file

	input: filename, list of database objects
	'''
	with open(dbo, 'w') as f:
		data = [ser_data.to_dict() for ser_data in dump_objects.all()]
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
	return None

def paperbackup():
	'''
	backups database by loading into json files, named by timestamp

	input: time script was run
	'''
	timestamp = int(time.time())
	backup_dir = os.path.join('/data4/paper/paperdata_backup', str(timestamp))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#tables = ('observation', 'file', 'feed', 'log')
	tables = ('observation', 'file', 'log')
	table_sorts = {'observation': {'first': 'julian_date', 'second': 'polarization'},
					'file': {'first': 'obsnum', 'second': 'filename'},
					'feed': {'first': 'julian_day', 'second': 'filename'},
					'log': {'first': 'timestamp', 'second': 'action'}}
	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		print(timestamp)
		for table in tables:
			db_file = '{table}_{timestamp}.json'.format(timestamp=timestamp)
			dbo = os.path.join(backup_dir, db_file)
			print(db_file)

			DB_table = getattr(pdbi, table.title())
			DB_dump = s.query(DB_table).order_by(getattr(DB_table, table_sorts[table]['first']).asc(),
												getattr(DB_table, table_sorts[table]['second']).asc())
			json_data(dbo, DB_dump)
			print('Table data backup saved')

	return None

if __name__ == '__main__':
	paperbackup()
