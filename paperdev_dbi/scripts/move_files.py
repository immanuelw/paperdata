#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdev

import sys
import time
import subprocess
import aipy as A
import hashlib
import glob
import socket
import os
import paramiko
import shutil
import psutil
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import paperdev_dbi as pdbi

### Script to move files and update paperdev database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def enough_space(required_space, space_path):
	free_space = psutil.disk_usage(space_path).free

	if required_space < free_space:
		return True

	return False

def email_space(table):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdev@gmail.com', 'papercomesfrom1tree')

	#Send the mail
	header = 'From: PAPERBridge <paperfeed.paperdev@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
	msgs = ''.join(header, '\nNot enough space for ', table, ' on folio')

	server.sendmail('paperfeed.paperdev@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdev@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdev@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	server.sendmail('paperfeed.paperdev@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

	server.quit()

	return None

def null_check(input_host, input_paths):
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(pdbi.File).filter(pdbi.File.host==input_host).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs)

	#for each input file, check if in filenames
	nulls = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	if len(nulls) > 0:
		return False

	return True

def rsync_copy(source, destination):
	subprocess.check_output(['rsync', '-ac', source, destination])
	return None

def move_files(input_host, input_paths, output_host, output_dir):
	named_host = socket.gethostname()
	destination = ''.join((output_host, ':', output_dir))
	action = 'move'
	table = 'file'
	if named_host == input_host:
		dbi = pdbi.DataBaseInterface()
		s = dbi.Session()
		for source in input_paths:
			rsync_copy(source, destination)
			full_path = ''.join((input_host, ':', source))
			FILE = dbi.get_file(full_path)
			timestamp = int(time.time())
			dbi.set_file_host(FILE.full_path, output_host)
			dbi.set_file_path(FILE.full_path, output_dir)
			dbi.set_file_time(FILE.full_path, timestamp)
			log_data = {'action':action,
						'table':table,
						'obsnum':None,
						'host':input_host,
						'full_path':source,
						'feed_path':None,
						'timestamp':timestamp}
			dbi.add_log(log_data)
			shutil.rmtree(source)
		s.close()
	else:
		dbi = pdbi.DataBaseInterface()
		s = dbi.Session()
		ssh = pdbi.login_ssh(output_host)
		for source in input_paths:
			rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
			rsync_del_command = '''rm -r {source}'''.format(source=source)
			ssh.exec_command(rsync_copy_command)
			full_path = ''.join((input_host, ':', source))
			FILE = dbi.get_file(full_path)
			timestamp = int(time.time())
			dbi.set_file_host(FILE.full_path, output_host)
			dbi.set_file_path(FILE.full_path, output_dir)
			dbi.set_file_time(FILE.full_path, timestamp)
			log_data = {'action':action,
						'table':table,
						'obsnum':None,
						'host':input_host,
						'full_path':source,
						'feed_path':None,
						'timestamp':timestamp}
			dbi.add_log(log_data)
			ssh.exec_command(rsync_del_command)
		ssh.close()
		s.close()

	print 'Completed transfer'
	return None

if __name__ == '__main__':
	named_host = socket.gethostname()
	input_host = raw_input('Source directory host: ')
	if named_host == input_host:
		input_paths = glob.glob(raw_input('Source directory path: '))
	else:
		ssh = pdbi.login_ssh(input_host)
		input_paths = raw_input('Source directory path: ')
		stdin, path_out, stderr = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
		input_paths = path_out.read().split('\n')[:-1]
		ssh.close()
		
	input_paths.sort()
	output_host = raw_input('Destination directory host: ')
	output_dir = raw_input('Destination directory: ')
	nulls = null_check(input_host, input_paths)
	if not nulls:
		#if any file not in db -- don't move anything
		print 'File(s) not in database'
		sys.exit()
	move_files(input_host, input_paths, output_host, output_dir)