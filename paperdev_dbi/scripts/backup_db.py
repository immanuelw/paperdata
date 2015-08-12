#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import sys
import time
import os
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import paperdev_dbi as pdbi
import json
import time

### Script to Backup paperdev database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

import decimal
def decimal_default(obj):
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def json_data(dbo, dump_objects):
	data = []
	with open(dbo, 'w') as f:
		for ser_data in dump_objects.all():
			data.append(ser_data.to_json())
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
	return None

def paperbackup(time_date):

	backup_dir = os.path.join('/data4/paper/paperdev_backup', str(time_date))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#Create separate files for each directory

	db1 = 'obs_{time_date}.json'.format(time_date=time_date)
	dbo1 = os.path.join(backup_dir, db1)
	print(dbo1)

	db2 = 'file_{time_date}.json'.format(time_date=time_date)
	dbo2 = os.path.join(backup_dir, db2)
	print(dbo2)

	#db3 = 'feed_{0}.json'.format(time_date)
	#dbo3 = os.path.join(backup_dir, db3)
	#print(dbo3)

	db4 = 'log_{0}.json'.format(time_date)
	dbo4 = os.path.join(backup_dir, db4)
	print(dbo4)

	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()

	OBS_dump = s.query(pdbi.Observation).order_by(pdbi.Observation.julian_date.asc(), pdbi.Observation.polarization.asc())
	json_data(dbo1, OBS_dump)

	FILE_dump = s.query(pdbi.File).order_by(pdbi.File.obsnum.asc(), pdbi.File.filename.asc())
	json_data(dbo2, FILE_dump)

	#FEED_dump = s.query(pdbi.Feed).order_by(pdbi.Feed.julian_day.asc(), pdbi.Feed.filename.asc())
	#json_data(dbo3, FEED_dump)

	LOG_dump = s.query(pdbi.Log).order_by(pdbi.Log.timestamp.asc(), pdbi.Log.action.asc())
	json_data(dbo3, LOG_dump)

	s.close()
	print 'Table data backup saved'

	return None

def email_backup(backup_file):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdev@gmail.com', 'papercomesfrom1tree')

	msg = MIMEMultipart()
	msg['Subject'] = 'PAPERDATA TABLE BACKUP'
	msg['From'] = 'paperfeed.paperdev@gmail.com'
	msg['To'] = 'paperfeed.paperdev@gmail.com'

	part = MIMEBase('application', 'octet-stream')
	part.set_payload(open(backup_file, 'rb').read())
	Encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment', filename=backup_file)
	msg.attach(part)

	#Send the mail
	server.sendmail('paperfeed.paperdev@gmail.com', 'paperfeed.paperdev@gmail.com', msg.as_string())

	server.quit()

	return None

if __name__ == '__main__':
	time_date = int(time.time())

	paperbackup(time_date)
	#backup_file = '/data4/paper/paperdev_backup/{time_date}/paperdev_backup.psv'.format(time_date=time_date)
	#email_backup(backup_file)
