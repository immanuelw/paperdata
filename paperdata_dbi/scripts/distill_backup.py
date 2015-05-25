#!/usr/bin/python
# -*- coding: utf-8 -*-
# Backup paperdistiller tables

from ddr_compress.dbi import DataBaseInterface, Observation, File, Neighbors, Log
import sys
import time
import csv
import os

### Script to Backup paperdistiller database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14
def paperdistiller_backup(time_date):

	backup_dir = os.path.join('/data4/paper/paperdistiller_backup', time_date)
	if not os.path.isdir(backup_dir):
		os.makedirs(backup_dir)

	#Create separate files for each directory

	db1 = 'file_backup_%s.psv'%(time_date)
	dbo1 = os.path.join(backup_dir,db1)
	print dbo1
	data_file1 = open(dbo1,'wb')
	wr1 = csv.writer(data_file1, delimiter='|', lineterminator='\n', dialect='excel')

	db2 = 'observation_backup_%s.psv'%(time_date)
	dbo2 = os.path.join(backup_dir,db2)
	print dbo2
	data_file2 = open(dbo2,'wb')
	wr2 = csv.writer(data_file2, delimiter='|', lineterminator='\n', dialect='excel')

	db3 = 'neighbors_backup_%s.psv'%(time_date)
	dbo3 = os.path.join(backup_dir,db3)
	print dbo3
	data_file3 = open(dbo3,'wb')
	wr3 = csv.writer(data_file3, delimiter='|', lineterminator='\n', dialect='excel')

	db4 = 'log_backup_%s.psv'%(time_date)
	dbo4 = os.path.join(backup_dir,db4)
	print dbo4
	data_file4 = open(dbo4,'wb')
	wr4 = csv.writer(data_file4, delimiter='|', lineterminator='\n', dialect='excel')


	dbi = DataBaseInterface()
	s = dbi.Session()

	quer = s.query(Observation).all()
	results = 
	for item in results:
		wr1.writerow(item)
	data_file1.close()

	quer = s.query(File).all()
	results = 
	for item in results:
		wr2.writerow(item)
	data_file2.close()

	quer = s.query(Neighbors).all()
	results = 
	for item in results:
		wr3.writerow(item)
	data_file3.close()

	quer = s.query(Log).all()
	results = 
	for item in results:
		wr4.writerow(item)
	data_file4.close()

	s.close()

	print time_date
	print 'Table data backup saved'

	return None

if __name__ == '__main__':
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	paperdistiller_backup(time_date)
