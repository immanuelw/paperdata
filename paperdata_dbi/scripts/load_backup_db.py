#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi as pdbi
import csv
import sys

### Script to create paperdata database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def load_backup_obs(backup_observation):
	dbi = pdbi.DataBaseInterface()
	with open(backup_observation, 'rb') as backup_obs:
		read = csv.reader(backup_obs, delimiter='|', lineterminator='\n', dialect='excel')
		for row in read:
			fixed_row = []
			for key, value in enumerate(row):
				if value == '':
					value = None
				else:
					if key in (0,3,4,10,11):
						value = int(value)
					elif key in (1,6,7,8,9):
						value = round(float(value), 5)
					elif key in (12,):
						if value == 'True':
							value = True
						elif value == 'False':
							value = False
					else:
						#item is string
						pass
				fixed_row.append(value)
			val = tuple(fixed_row)
			print row
			print val
			dbi.add_observation(*val)
	return None

def check_row(row, s):
	full_path = row[4]
	if full_path in ('/nas1/data/psa6644/zen.2456644.18779.xy.uv','/data3/data1/paper/psa/psa228/zen.2440587.69719.uv',
					'/data3/data1/paper/psa/psa228/zen.2455228.00925.uv',
					'/data3/data2/home/jacobsda/tmp/vartestspsa/test1/zen.2455820.12187.uv',
					'/data3/paper/fiducial_night/zen.2455906.07399.uv', '/data3/paper/jkris/560x/5600/zen.2455600.20624.uv',
					'/data3/paper/jkris/560x/5600/zen.2455600.22016.uv', '/data3/paper/jkris/560x/5600/zen.2455600.26192.uv'):
		return None
	if full_path == '/data4/paper/damo/PolImaging/batch_FHD/zen.test.uv':
		obsnum = 17185731181
		full_row = row[:5] + [obsnum] + row[6:]
		return full_row
	jd = row[2]
	try:
		julian_date = round(float('.'.join(row[2].split('.')[1:3]).split('_')[0]), 5)
	except:
		julian_date = round(float('.'.join(row[2].split('.')[1:3]).split('_')[0].split('.uv')[0]), 5)
	try:
		polarization = row[2].split('.')[3]
		if polarization not in ('xx', 'xy', 'yx', 'yy'):
			polarization = 'all'
	except:
		polarization = 'all'
	try:
		OBS = s.query(pdbi.Observation).filter(pdbi.Observation.julian_date==julian_date).filter(pdbi.Observation.polarization==polarization).one()
		obsnum = OBS.obsnum
	except:
		obsnum = -1
	full_row = row[:5] + [obsnum] + row[6:]

	return full_row

def load_backup_files(backup_fi):
	dbi = pdbi.DataBaseInterface()
	with open(backup_fi, 'rb') as backup_file:
		read = csv.reader(backup_file, delimiter='|', lineterminator='\n', dialect='excel')
		s = dbi.Session()
		for row in read:
			# XXXX comment out once finished making show old files are fine
			row = check_row(row, s)
			if row is None:
				continue
			print row
			fixed_row = []
			for key, value in enumerate(row):
				if value == '':
					value = None
				else:
					if key in (5,):
						value = int(value)
					elif key in (6,):
						value = round(float(value), 1)
					elif key in (9,10):
						if value == 'True':
							value = True
						elif value == 'False':
							value = False
					else:
						#item is string
						pass	
				fixed_row.append(value)
			val = tuple(fixed_row)
			dbi.add_file(*val)
		s.close()
	return None

if __name__ == '__main__':
	if len(sys.argv) == 3:
		backup_obs = sys.argv[1]
		backup_file = sys.argv[2]
	else:
		backup_obs = '/data4/paper/paperdata_backup/obs_v2.psv'
		backup_file = '/data4/paper/paperdata_backup/file_v1.psv'
	
	#load_backup_obs(backup_obs)
	load_backup_files(backup_file)
