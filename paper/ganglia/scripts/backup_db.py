#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
from __future__ import print_function
import os
import sys
import time
import json
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import dbi as pyg

### Script to Backup pyganglia database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

import decimal
def decimal_default(obj):
	'''
	fixes decimal issue with json module
	'''
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def json_data(dbo, dump_objects):
	'''
	dumps list of objects into a json file

	input: filename, list of database objects
	'''
	data = []
	with open(dbo, 'w') as f:
		for ser_data in dump_objects.all():
			data.append(ser_data.to_dict())
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
	return None

def paperbackup(timestamp):
	'''
	backups database by loading into json files, named by timestamp

	input: time script was run
	'''
	backup_dir = os.path.join('/data4/paper/pyganglia_backup', str(timestamp))
	if not os.path.isdir(backup_dir):
		os.mkdir(backup_dir)

	#Create separate files for each directory

	db1 = 'filesystem_{timestamp}.json'.format(timestamp=timestamp)
	dbo1 = os.path.join(backup_dir, db1)
	print(dbo1)

	db2 = 'monitor_{timestamp}.json'.format(timestamp=timestamp)
	dbo2 = os.path.join(backup_dir, db2)
	print(dbo2)

	db3 = 'iostat_{timestamp}.json'.format(timestamp=timestamp)
	dbo3 = os.path.join(backup_dir, db3)
	print(dbo3)

	db4 = 'ram_{timestamp}.json'.format(timestamp=timestamp)
	dbo4 = os.path.join(backup_dir, db4)
	print(dbo4)

	db5 = 'cpu_{timestamp}.json'.format(timestamp=timestamp)
	dbo5 = os.path.join(backup_dir, db5)
	print(dbo5)

	dbi = pyg.DataBaseInterface()
	s = dbi.Session()

	table = getattr(pyg, 'Filesystem')
	FILESYSTEM_dump = s.query(table).orderby(getattr(table, 'timestamp').asc(), getattr(table, 'host').asc(), getattr(table, 'system').asc())
	json_data(dbo1, FILESYSTEM_dump)

	table = getattr(pyg, 'Monitor')
	MONITOR_dump = s.query(table).orderby(getattr(table, 'timestamp').asc(), getattr(table, 'host').asc(), getattr(table, 'filename').asc())
	json_data(dbo2, MONITOR_dump)

	table = getattr(pyg, 'Iostat')
	IOSTAT_dump = s.query(table).orderby(getattr(table, 'timestamp').asc(), getattr(table, 'host').asc(), getattr(table, 'device').asc())
	json_data(dbo1, IOSTAT_dump)

	table = getattr(pyg, 'Ram')
	RAM_dump = s.query(table).orderby(getattr(table, 'timestamp').asc(), getattr(table, 'host').asc())
	json_data(dbo1, RAM_dump)

	table = getattr(pyg, 'Cpu')
	CPU_dump = s.query(table).orderby(getattr(table, 'timestamp').asc(), getattr(table, 'host').asc(), getattr(table, 'cpu').asc())
	json_data(dbo1, CPU_dump)

	s.close()
	print('Table data backup saved')

	return None

def email_backup(backup_file):
	'''
	emails backup file(s) to gmail address

	input: name of backup file
	'''
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.pyganglia@gmail.com', 'papercomesfrom1tree')

	msg = MIMEMultipart()
	msg['Subject'] = 'PAPERDATA TABLE BACKUP'
	msg['From'] = 'paperfeed.pyganglia@gmail.com'
	msg['To'] = 'paperfeed.pyganglia@gmail.com'

	part = MIMEBase('application', 'octet-stream')
	part.set_payload(open(backup_file, 'rb').read())
	Encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment', filename=backup_file)
	msg.attach(part)

	#Send the mail
	server.sendmail('paperfeed.pyganglia@gmail.com', 'paperfeed.pyganglia@gmail.com', msg.as_string())

	server.quit()

	return None

if __name__ == '__main__':
	timestamp = int(time.time())

	paperbackup(timestamp)
	#backup_file = '/data4/paper/pyganglia_backup/{timestamp}/pyganglia_backup.json'.format(timestamp=timestamp)
	#email_backup(backup_file)
