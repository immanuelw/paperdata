#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paper tables

from __future__ import print_function
import sys
import json
import glob
from paper.data import dbi as pdbi, data_db as pdb
import sqlalchemy.exc

### Script to create paper database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def load_backup(backup_file=None, table=None):
	'''
	loads backups from json into database

	input: name of backup file, table name
	'''
	if table is None:
		return None
	if backup_file is None:
		backup_list = glob.glob('/data4/paper/paperdata_backup/[0-9]*')
		backup_list.sort(reverse=True)
		timestamp = int(backup_list[0].split('/')[-1])
		backup_file = '/data4/paper/paperdata_backup/{timestamp}/{table}_{timestamp}.json'.format(table=table, timestamp=timestamp)

	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	with open(backup, 'r') as backup_db:
		read = json.load(backup_db)
		for row in read:
			print(row.items())
			try:
				dbi.add_to_table(s, table, row)
			except KeyboardInterrupt:
				raise
			except:
				print('Failed to load in entry')
	s.close()

	return None

if __name__ == '__main__':
	if len(sys.argv) == 3:
		backup_table = sys.argv[1]
		backup_file = sys.argv[2]
		load_backup(backup_file, table=backup_table)
	else:
		#load_backup(table='observation')
		load_backup(table='file')
		#load_backup(table='feed')
		#load_backup(table='log')
		#load_backup(table='rtp_file')
