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
import paper as ppdata
from paper.ganglia import dbi as pyg

### Script to Backup pyganglia database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

def json_data(dbo, dump_objects):
	'''
	dumps list of objects into a json file

	Parameters
	----------
	dbo (str): filename
	dump_objects (list): database objects query
	'''
	with open(dbo, 'w') as f:
		data = [ser_data.to_dict() for ser_data in dump_objects.all()]
		json.dump(data, f, sort_keys=True, indent=1, default=ppdata.decimal_default)

	return None

def paperbackup(dbi):
	'''
	backups database by loading into json files, named by timestamp

	Parameters
	----------
	dbi (object): database interface object
	'''
	timestamp = int(time.time())
	backup_dir = os.path.join('/data4/paper/pyganglia_backup', str(timestamp))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	tables = ('Filesystem', 'Monitor', 'Iostat', 'Ram', 'Cpu')
	table_sorts = {'Filesystem': {'first': 'timestamp', 'second': 'host', 'third': 'system'},
					'Monitor': {'first': 'timestamp', 'second': 'host', 'third': 'filename'},
					'Iostat': {'first': 'timestamp', 'second': 'host', 'third': 'device'},
					'Ram': {'first': 'timestamp', 'second': 'host', 'third': 'total'},
					'Cpu': {'first': 'timestamp', 'second': 'host', 'third': 'cpu'}}
	with dbi.session_scope as s:
		print(timestamp)
		for table in tables:
			db_file = '{table}_{timestamp}.json'.format(table=table, timestamp=timestamp)
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
	dbi = pyg.DataBaseInterface()
	paperbackup(dbi)
