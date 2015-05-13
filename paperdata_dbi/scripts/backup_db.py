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
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

### Script to Backup paperdata database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14
def paperbackup(time_date, usrnm, pswd):

	backup_dir = os.path.join('/data4/paper/paperdata_backup', time_date)
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#Create separate files for each directory

	db1 = 'paper_observation_backup_%s.psv'%(time_date)
	dbo1 = os.path.join(backup_dir,db1)
	print dbo1
	data_file1 = open(dbo1,'wb')
	wr1 = csv.writer(data_file1, delimiter='|', lineterminator='\n', dialect='excel')

	db2 = 'paper_file_backup_%s.psv'%(time_date)
	dbo2 = os.path.join(backup_dir,db2)
	print dbo2
	data_file2 = open(dbo2,'wb')
	wr2 = csv.writer(data_file2, delimiter='|', lineterminator='\n', dialect='excel')

	#db3 = 'paperrename_backup_%s.psv'%(time_date)
	#dbo3 = os.path.join(backup_dir,db3)
	#print dbo3
	#data_file3 = open(dbo3,'wb')
	#wr3 = csv.writer(data_file3, delimiter='|', lineterminator='\n', dialect='excel')

	#db4 = 'paperfeed_backup_%s.psv'%(time_date)
	#dbo4 = os.path.join(backup_dir,db4)
	#print dbo4
	#data_file4 = open(dbo4,'wb')
	#wr4 = csv.writer(data_file4, delimiter='|', lineterminator='\n', dialect='excel')

	#Load data into named database and table
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute('SELECT * FROM Observation order by julian_date asc, polarization asc')
	results = cursor.fetchall()

	for item in results:
		wr1.writerow(item)
	data_file1.close()

	cursor.execute('SELECT * FROM File order by obsnum asc, filename asc')
	results = cursor.fetchall()

	for item in results:
		wr2.writerow(item)
	data_file2.close()

	#cursor.execute('SELECT * FROM paperrename order by julian_day asc, raw_path asc')
	#results = cursor.fetchall()

	#for item in results:
	#	#wr3.writerow(item)
	#data_file3.close()

	#cursor.execute('SELECT * FROM paperfeed order by julian_day asc, raw_path asc')
	#results = cursor.fetchall()

	#for item in results:
	#	#wr4.writerow(item)
	#data_file4.close()

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

def email_backup(backup_file):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	msg = MIMEMultipart()
	msg['Subject'] = 'PAPERDATA TABLE BACKUP'
	msg['From'] = 'paperfeed.paperdata@gmail.com'
	msg['To'] = 'paperfeed.paperdata@gmail.com'

	part = MIMEBase('application', 'octet-stream')
	part.set_payload(open(backup_file, 'rb').read())
	Encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment', filename=backup_file)
	msg.attach(part)

	#Send the mail
	server.sendmail('paperfeed.paperdata@gmail.com', 'paperfeed.paperdata@gmail.com', msg.as_string())

	server.quit()

	return None

if __name__ == '__main__':
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")

	full = raw_input('Backup through .sql?(y/n): ')
	if full == 'y':
		dbnum = '/data2/home/immwa/scripts/paperdata/backups/paperdata_%s.sql'%(time_date)
		usrnm = 'immwa'
		pswd = getpass.getpass('Password: ')
		sql_backup(dbnum, time_date, usrnm, pswd)
	else:
		usrnm = 'paperboy'
		pswd = 'paperboy'
		paperbackup(time_date, usrnm, pswd)
		backup_file = '/data4/paper/paperdata_backup/%s/paperdata_backup_%s.psv' %(time_date, time_date)
		email_backup(backup_file)
