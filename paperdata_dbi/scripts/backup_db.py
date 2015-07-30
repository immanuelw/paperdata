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
import paperdata_dbi as pdbi
import json

### Script to Backup paperdata database
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
			s_dict = ser_data.__dict__
			try:
				s_dict.pop('_sa_instance_state')
			except:
				pass
			data.append(s_dict)
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
	return None

def paperbackup(time_date):

	backup_dir = os.path.join('/data4/paper/paperdata_backup', time_date)
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#Create separate files for each directory

	db1 = 'obs_{time_date}.json'.format(time_date=time_date)
	dbo1 = os.path.join(backup_dir, db1)
	print dbo1

	db2 = 'file_{time_date}.json'.format(time_date=time_date)
	dbo2 = os.path.join(backup_dir, db2)
	print dbo2

	#db3 = 'feed_{0}.json'.format(time_date)
	#dbo3 = os.path.join(backup_dir, db3)
	#print dbo3

	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()

	OBS_dump = s.query(pdbi.Observation).order_by(pdbi.Observation.julian_date.asc(), pdbi.Observation.polarization.asc())
	json_data(dbo1, OBS_dump)

	FILE_dump = s.query(pdbi.File).order_by(pdbi.File.obsnum.asc(), pdbi.File.filename.asc())
	json_data(dbo2, FILE_dump)

	#FEED_dump = s.query(pdbi.Feed).order_by(pdbi.Feed.julian_day.asc(), pdbi.Feed.filename.asc())
	#json_data(dbo3, FEED_dump)

	s.close()
	print time_date
	print 'Table data backup saved'

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
	time_date = int(time.time())

	paperbackup(time_date)
	#backup_file = '/data4/paper/paperdata_backup/{time_date}/paperdata_backup.psv'.format(time_date=time_date)
	#email_backup(backup_file)
