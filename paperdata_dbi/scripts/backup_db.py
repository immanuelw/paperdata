#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import sys
import time
import csv
import os
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import paperdata_dbi as pdbi

### Script to Backup paperdata database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14
def paperbackup(time_date):

	backup_dir = os.path.join('/data4/paper/paperdata_backup', time_date)
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#Create separate files for each directory

	db1 = 'obs_{0}.psv'.format(time_date)
	dbo1 = os.path.join(backup_dir,db1)
	print dbo1
	data_file1 = open(dbo1,'wb')
	wr1 = csv.writer(data_file1, delimiter='|', lineterminator='\n', dialect='excel')

	db2 = 'file_{0}.psv'.format(time_date)
	dbo2 = os.path.join(backup_dir,db2)
	print dbo2
	data_file2 = open(dbo2,'wb')
	wr2 = csv.writer(data_file2, delimiter='|', lineterminator='\n', dialect='excel')

	# execute the SQL query using execute() method.
	OBSs = s.query(pdbi.Observation).order_by(pdbi.Observation.julian_date.asc(), pdbi.Observation.polarization.asc()).all()

	for OBS in OBSs:
		item = (OBS.obsnum, OBS.julian_date, OBS.polarization, OBS.julian_day, OBS.era, OBS.era_type, OBS.length,
				OBS.time_start, OBS.time_end, OBS.delta_time, OBS.prev_obs, OBS.next_obs, OBS.edge)
		wr1.writerow(item)
	data_file1.close()

	FILEs = s.query(pdbi.File).order_by(pdbi.File.obsnum.asc(), pdbi.File.filename.asc()).all()

	for FILE in FILEs:
		item = (FILE.host, FILE.path, FILE.filename, FILE.filetype, FILE.full_path, FILE.obsnum, FILE.filesize, FILE.md5,
				FILE.tape_index, FILE.write_to_tape, FILE.delete_file)
		wr2.writerow(item)
	data_file2.close()

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
		paperbackup(time_date)
		backup_file = '/data4/paper/paperdata_backup/%s/paperdata_backup_%s.psv' %(time_date, time_date)
		email_backup(backup_file)
