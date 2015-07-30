#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi as pdbi
import csv
import sys
import json
import paperdata_db as pdb

### Script to create paperdata database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def load_backup(backup, table=None):
	dbi = pdbi.DataBaseInterface()
	with open(backup, 'r') as backup_db:
		read = json.load(backup_db)
		if table is None:
			return None
		elif table == 'observation':
			OBS_class = pdb.Observation()
			obs_list = OBS_class.db_list
			for row in read:
				print tuple(row)
				try:
					dbi.add_observation(**row)
				except:
					print('Failed to load in entry')
		elif table == 'file':
			FILE_class = pdb.File()
			file_list = FILE_class.db_list
			for row in read:
				print tuple(row)
				try:
					dbi.add_file(**row)
				except:
					print('Failed to load in entry')
		#elif table == 'feed':
		#	FEED_class = pdb.Feed()
		#	feed_list = FEED_class.db_list
		#	for row in read:
		#		print tuple(row)
		#		try:
		#			dbi.add_file(**row)
		#		except:
		#			print('Failed to load in entry')

	return None

if __name__ == '__main__':
	if len(sys.argv) == 3:
		backup_obs = sys.argv[1]
		backup_file = sys.argv[2]
	else:
		backup_obs = '/data4/paper/paperdata_backup/obs_v2.psv'
		backup_file = '/data4/paper/paperdata_backup/file_v1.psv'
	
	#load_backup(backup_obs, table='observation')
	load_backup(backup_file, table='file')
