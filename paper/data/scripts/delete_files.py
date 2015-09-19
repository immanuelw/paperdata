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
import paper as ppdata
from paper.data import dbi as pdbi

### Script to move files and update paper database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def delete_check(input_host):
	'''
	checks for which files can be deleted

	input: host of system
	output: list of uv* file paths of files to be deleted
	'''
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	table = getattr(pdbi, 'File')
	FILEs = s.query(table).filter(getattr(table, 'delete_file') == True).filter(getattr(table, 'tape_index') != None)\
							.filter(getattr(table, 'host') == input_host).all()
	s.close()
	#all files on same host
	full_paths = tuple(os.path.join(getattr(FILE, 'path'), getattr(FILE, 'filename')) for FILE in FILEs)
	return full_paths

def set_delete_table(input_host, source, output_host, output_dir):
	'''
	updates table for deleted file

	input: user host, source file, output host, output directory
	'''
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	action = 'delete'
	table = 'file'
	full_path = ''.join((input_host, ':', source))
	timestamp = int(time.time())
	FILE = dbi.get_entry(s, 'file', full_path)
	dbi.set_entry(s, FILE, 'host', output_host)
	dbi.set_entry(s, FILE, 'path', output_dir)
	dbi.set_entry(s, FILE, 'delete_file', False)
	dbi.set_entry(s, FILE, 'timestamp', timestamp)
	identifier = full_path
	log_data = {'action':action,
				'table':table,
				'identifier':identifier,
				'timestamp':timestamp}
	dbi.add_to_table(s, 'log', log_data)
	s.close()
	return None

def rsync_copy(source, destination):
	'''
	uses rsync to copy files and make sure they have not changed

	input: source file path, destination path
	'''
	subprocess.check_output(['rsync', '-ac', source, destination])
	return None

def delete_files(input_host, input_paths, output_host, output_dir):
	'''
	delete files

	input: file host, list of file paths, output host, output directory
	'''
	named_host = socket.gethostname()
	destination = ''.join((output_host, ':', output_dir))
	if named_host == input_host:
		for source in input_paths:
			rsync_copy(source, destination)
			set_delete_table(input_host, source, output_host, output_dir)
			shutil.rmtree(source)
	else:
		ssh = ppdata.login_ssh(host)
		for source in input_paths:
			rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
			rsync_del_command = '''rm -r {source}'''.format(source=source)
			ssh.exec_command(rsync_copy_command)
			set_delete_table(input_host, source, output_host, output_dir)
			ssh.exec_command(rsync_del_command)
		ssh.close()

	print('Completed transfer')
	return None

if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	output_host = raw_input('Destination directory host: ')
	output_dir = raw_input('Destination directory: ')
	input_paths = delete_check(input_host)
	delete_files(input_host, input_paths, output_host, output_dir)
