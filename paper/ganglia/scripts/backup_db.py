#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
from __future__ import print_function
import os
import sys
import time
import json
import decimal
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
from paper.ganglia import dbi as pyg

### Script to Backup pyganglia database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

def decimal_default(obj):
	'''
	fixes decimal issue with json module

	Args:
		obj (object)

	Returns:
		object: float version of decimal object
	'''
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def json_data(dbo, dump_objects):
	'''
	dumps list of objects into a json file

	Args:
		dbo (str): filename
		dump_objects (list): database objects query
	'''
	with open(dbo, 'w') as f:
		data = [ser_data.to_dict() for ser_data in dump_objects.all()]
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)

	return None

def paperbackup():
	'''
	backups database by loading into json files, named by timestamp
	'''
	timestamp = int(time.time())
	backup_dir = os.path.join('/data4/paper/pyganglia_backup', str(timestamp))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	tables = ('filesystem', 'monitor', 'iostat', 'ram', 'cpu')
	table_sorts = {'filesystem': {'first': 'timestamp', 'second': 'host', 'third': 'system'},
					'monitor': {'first': 'timestamp', 'second': 'host', 'third': 'filename'},
					'iostat': {'first': 'timestamp', 'second': 'host', 'third': 'device'},
					'ram': {'first': 'timestamp', 'second': 'host', 'third': 'total'},
					'cpu': {'first': 'timestamp', 'second': 'host', 'third': 'cpu'}}
	dbi = pyg.DataBaseInterface()
	with dbi.session_scope as s:
		print(timestamp)
		for table in tables:
			db_file = '{table}_{timestamp}.json'.format(timestamp=timestamp)
			dbo = os.path.join(backup_dir, db_file)
			print(db_file)

			DB_table = getattr(pyg, table.title())
			DB_dump = s.query(DB_table).order_by(getattr(DB_table, table_sorts[table]['first']).asc(),
												getattr(DB_table, table_sorts[table]['second']).asc(),
												getattr(DB_table, table_sorts[table]['third']).asc())
			json_data(dbo, DB_dump)
			print('Table data backup saved')

	return None

if __name__ == '__main__':
	paperbackup()
