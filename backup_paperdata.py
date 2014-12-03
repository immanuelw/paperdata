#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import time
import csv
import subprocess

### Script to Backup paperdata database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

def backup_paperdata(dbnum, time_date):
	print dbnum
	resultFile = open(dbnum,'wb')
	resultFile.close()

	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('SELECT * FROM paperdata order by julian_date asc, raw_location asc, path asc')
	results = cursor.fetchall()

	resultFile = open(dbnum,'ab')
	wr = csv.writer(resultFile, delimiter='|', dialect='excel')

	for item in results:
		wr.writerow(item)
	resultFile.close()

	print time_date
	print 'Table data backup saved'

	# Close the cursor object
	cursor.close()
	connection.close()

	return None

def sql_backup(dbnum, time_date, usrnm, pswd):
	print dbnum
	file = open(dbnum, 'wb')
	subprocess.call(['mysqldump', '-h', 'shredder', '-u', usrnm, '--password=%s'%(pswd), 'paperdata'], stdout=file)
	file.close()

	print time_date
	print 'Paperdata database backup saved'
	return None

if __name__ == '__main__':
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")

	full = raw_input('Backup entire database?(y/n): ')
	if full == 'y':
		dbnum = '/data2/home/immwa/scripts/paperdata/backups/paperdata_%s.sql'%(time_date)
		usrnm = 'immwa'
		pswd = getpass.getpass('Password: ')
		sql_backup(dbnum, time_date, usrnm, pswd)
	else:
		dbnum = '/data2/home/immwa/scripts/paperdata/backups/paperdata_backup_%s.psv'%(time_date)
		backup_paperdata(dbnum, time_date)
