#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdata

import MySQLdb
import sys
import getpass
import time
import csv
import subprocess
import aipy as A
import hashlib
import glob
import socket
import os
import paramiko
import paperdata_dbi
import shutil

### Script to move files and update paperdata database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def null_check(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).filter(dbi.File.host==input_host).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs)

	#for each input file, check if in filenames
	nulls = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	if len(nulls) > 0:
		return False

	return True

def rsync_m(source, destination):
	subprocess.check_output(['rsync', '-a', '--remove-source-files', source, destination])
	return None

def move_files(input_host, input_paths, output_host, output_dir):
	named_host = socket.gethostname()
	destination = output_host + ':' + output_dir
	if named_host == input_host:
		if input_host == output_host:
			dbi = paperdata_dbi.DataBaseInterface()
			s = dbi.Session()
			for source in input_paths:
				shutil.move(source, output_dir)
				#change in database
				full_path = input_host + ':' + source
				FILE = dbi.get_file(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
			s.close()
		else:
			dbi = paperdata_dbi.DataBaseInterface()
			s = dbi.Session()
			for source in input_paths:
				rsync_m(source, destination)
				full_path = input_host + ':' + source
				FILE = dbi.get_file(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
			s.close()
	else:
		if input_host == output_host:
			dbi = paperdata_dbi.DataBaseInterface()
			s = dbi.Session()
			ssh = paperdata_dbi.login_ssh(host)
			sftp = ssh.open_sftp()
			for source in input_paths:
				sftp.rename(source, output_dir)
				full_path = input_host + ':' + source
				FILE = dbi.get_file(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
			sftp.close()
			ssh.close()
			s.close()
		else:
			dbi = paperdata_dbi.DataBaseInterface()
			s = dbi.Session()
			ssh = paperdata_dbi.login_ssh(host)
			for source in input_paths:
				rsync_move = '''rsync -a --remove-source-files {source} {destination}'''.format(source=source, destination=destination)
				ssh.exec_command(rsync_move)
				full_path = input_host + ':' + source
				FILE = dbi.get_file(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
			ssh.close()
			s.close()

	print 'Completed transfer'
	return None

if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	input_paths = glob.glob(raw_input('Source directory path: '))
	input_paths.sort()
	output_host = raw_input('Destination directory host: ')
	output_dir = raw_input('Destination directory: ')
	nulls = null_check(input_host, input_paths)
	if not null:
		#if any file not in db -- don't move anything
		print 'File(s) not in database'
		sys.exit()
	move_files(input_host, input_paths, output_host, output_dir)
