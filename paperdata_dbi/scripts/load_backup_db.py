#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi
import csv

### Script to create paperdata database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def load_backup_db(backup_observation, backup_file):
	dbi = paperdata_dbi.DataBaseInterface()
	with open(outfile, 'rb') as backup_obs:
		read = csv.reader(backup_file, delimiter='|', lineterminator='\n', dialect='excel')
		for row in read:
			val = tuple(row)
			dbi.add_observation(*val)
	with open(outfile, 'rb') as backup_file:
		read = csv.reader(backup_file, delimiter='|', lineterminator='\n', dialect='excel')
		for row in read:
			val = tuple(row)
			dbi.add_file(*val)

	print 'Table data loaded.'

if __name__ == '__main__':
	if len(sys.argv) == 3
		backup_obs = sys.argv[1]
		backup_file = sys.argv[2]
	else:
		backup_obs = '/data4/paper/paperdata_backup/paper_observation_backup_v1.psv'
		backup_file = '/data4/paper/paperdata_backup/paper_file_backup_v1.psv'
	
	load_backup_db(backup_file)
