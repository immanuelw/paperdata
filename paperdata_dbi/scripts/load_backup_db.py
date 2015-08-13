#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi as pdbi
import sys
import json
import paperdata_db as pdb
import glob
import sqlalchemy.exc

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
		else:
			for row in read:
				print row.items()
				try:
					if table == 'observation':
						dbi.add_observation(row)
					elif table == 'file':
						dbi.add_file(row)
					#elif table == 'feed':
					#	dbi.add_feed(row)
					elif table == 'log':
						dbi.add_log(row)
				except:
					print('Failed to load in entry')

	return None

if __name__ == '__main__':
	if len(sys.argv) == 3:
		backup_obs = sys.argv[1]
		backup_file = sys.argv[2]
	else:
		backup_list = glob.glob('/data4/paper/paperdata_backup/[0-9]*')
		backup_list.sort(reverse=True)
		backup_dir = backup_list[0]
		time_date = int(backup_dir.split('/')[-1])
		backup_obs = '/data4/paper/paperdata_backup/{time_date}/obs_{time_date}.json'.format(time_date=time_date)
		backup_file = '/data4/paper/paperdata_backup/{time_date}/file_{time_date}.json'.format(time_date=time_date)
		backup_feed = '/data4/paper/paperdata_backup/{time_date}/feed_{time_date}.json'.format(time_date=time_date)
		backup_log = '/data4/paper/paperdata_backup/{time_date}/log_{time_date}.json'.format(time_date=time_date)
		
	
	#load_backup(backup_obs, table='observation')
	load_backup(backup_file, table='file')
	#load_backup(backup_feed, table='feed')
	#load_backup(backup_log, table='feed')
