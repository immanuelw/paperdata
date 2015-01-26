#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import time
import csv
import base64
import os
import subprocess

### Script to Backup paperdistiller database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14
def paperdistiller_backup(time_date, usrnm, pswd):

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

	#Load data into named database and table
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute('SELECT * FROM file')
	results = cursor.fetchall()

	for item in results:
		wr1.writerow(item)
	data_file1.close()

	cursor.execute('SELECT * FROM observation')
	results = cursor.fetchall()

	for item in results:
		wr2.writerow(item)
	data_file2.close()

	cursor.execute('SELECT * FROM neighbors')
	results = cursor.fetchall()

	for item in results:
		wr3.writerow(item)
	data_file3.close()

	cursor.execute('SELECT * FROM log')
	results = cursor.fetchall()

	for item in results:
		wr4.writerow(item)
	data_file4.close()

	print time_date
	print 'Table data backup saved'

	# Close the cursor object
	cursor.close()
	connection.close()

	return None

def sql_backup(time_date, usrnm, pswd):
	backup_dir = os.path.join('/data4/paper/paperdistiller_backup', time_date)
	if not os.path.isdir(backup_dir):
		os.makedirs(backup_dir)

	num = 'paperdistiller_%s.sql'%(time_date)
	dbnum = os.path.join(backup_dir, num)

	print dbnum
	file = open(dbnum, 'wb')
	subprocess.call(['mysqldump', '-h', 'shredder', '-u', usrnm, '--password=%s'%(pswd), 'paperdistiller'], stdout=file)
	file.close()

	print time_date
	print 'Paperdata database backup saved'
	return None

if __name__ == '__main__':
	usrnm = raw_input('Input username: ')
	pswd = getpass.getpass('Password: ')

	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")

	full = raw_input('Backup entire database(y/n): ')

	if full == 'y':
		sql_backup(time_date, usrnm, pswd)
	else:
		paperdistiller_backup(time_date, usrnm, pswd)
