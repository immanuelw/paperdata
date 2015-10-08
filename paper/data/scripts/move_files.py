#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paper

from __future__ import print_function
import sys
import time
import subprocess
import glob
import socket
import os
import shutil
import psutil
import paper as ppdata
from paper.data import dbi as pdbi
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

### Script to move files and update paper database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def enough_space(required_space, space_path):
	'''
	checks path for enough space

	Parameters
	----------
	required_space | int: amount of space needed in bytes
	space_path | str: path to check for spacce

	Returns
	-------
	bool: is there enough space
	'''
	free_space = psutil.disk_usage(space_path).free

	if required_space < free_space:
		return True

	return False

def email_space(table):
	'''
	emails people if there is not enough space on folio

	Parameters
	----------
	table | str: table name
	'''
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paper@gmail.com', 'papercomesfrom1tree')

	#Send the mail
	header = 'From: PAPERBridge <paperfeed.paper@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
	msgs = ''.join((header, '\nNot enough space for ', table, ' on folio'))

	server.sendmail('paperfeed.paper@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paper@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paper@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	server.sendmail('paperfeed.paper@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

	server.quit()

	return None

def null_check(dbi, input_host, input_paths):
	'''
	checks if file(s) is(are) in database

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: host of files
	input_paths | list: uv* file paths

	Returns
	-------
	bool: are there any files not in database -- True if there are None
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'host') == input_host).all()
	#all files on same host
	filenames = tuple(os.path.join(getattr(FILE, 'path'), getattr(FILE, 'filename')) for FILE in FILEs)

	#for each input file, check if in filenames
	nulls = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	if len(nulls) > 0:
		return False

	return True

def set_move_table(s, dbi, input_host, source, output_host, output_dir):
	'''
	updates table for moved file

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	input_host | str: user host
	source | str: source file
	output_host | str: output host
	output_dir | str: output directory
	'''
	full_path = ''.join((input_host, ':', source))
	timestamp = int(time.time())
	FILE = dbi.get_entry(s, 'File', full_path)
	dbi.set_entry(s, FILE, 'host', output_host)
	dbi.set_entry(s, FILE, 'path', output_dir)
	dbi.set_entry(s, FILE, 'timestamp', timestamp)
	identifier = getattr(FILE, 'full_path')
	log_data = {'action': 'move',
				'table': 'file',
				'identifier': identifier,
				'timestamp': timestamp}
	dbi.add_entry_dict(s, 'Log', log_data)

	return None

def move_files(dbi, input_host=None, input_paths=None, output_host=None, output_dir=None):
	'''
	move files

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: file host --defaults to None
	input_paths | list: file paths --defaults to None
	output_host | str: output host --defaults to None
	output_dir | str: output directory --defaults to None
	'''
	named_host = socket.gethostname()
	input_host = raw_input('Source directory host: ') if input_host is None else input_host
	output_host = raw_input('Destination directory host: ') if output_host is None else output_host
	output_dir = raw_input('Destination directory: ') if output_dir is None else output_dir

	if input_paths is None:
		if named_host == input_host:
			input_paths = sorted(glob.glob(raw_input('Source directory path: ')))
		else:
			with ppdata.ssh_scope(host) as ssh:
				input_paths = raw_input('Source directory path: ')
				_, path_out, _ = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
				input_paths = sorted(path_out.read().split('\n')[:-1])
	
	nulls = null_check(input_host, input_paths)
	if not nulls:
		#if any file not in db -- don't move anything
		print('File(s) not in database')
		return None

	destination = ''.join((output_host, ':', output_dir))
	if named_host == input_host:
		with dbi.session_scope() as s:
			for source in input_paths:
				ppdata.rsync_copy(source, destination)
				set_move_table(s, dbi, input_host, source, output_host, output_dir)
				shutil.rmtree(source)
	else:
		with ppdata.ssh_scope(input_host) as ssh:
			for source in input_paths:
				rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
				rsync_del_command = '''rm -r {source}'''.format(source=source)
				ssh.exec_command(rsync_copy_command)
				set_move_table(input_host, source, output_host, output_dir)
				ssh.exec_command(rsync_del_command)
	print('Completed transfer')

	return None

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	move_files(dbi)
