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

def delete_check(input_host):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).filter(dbi.File.delete_file==True).filter(dbi.File.tape_index!=None).filter(dbi.File.host==input_host).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs)
		
	return filenames

def rsync_m(source, destination):
	subprocess.check_output(['rsync', '-a', '--remove-source-files', source, destination])
	return None

def delete_files(input_host, input_paths, output_host, output_dir):
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
				dbi.set_file_delete(FILE.full_path, False)
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
				dbi.set_file_delete(FILE.full_path, False)
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
				try:
					sftp.rmdir(full_path)
				except:
					sftp.remove(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
				dbi.set_file_delete(FILE.full_path, False)
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
				try:
					sftp.rmdir(full_path)
				except:
					sftp.remove(full_path)
				dbi.set_file_host(FILE.full_path, output_host)
				dbi.set_file_path(FILE.full_path, output_dir)
				dbi.set_file_delete(FILE.full_path, False)
			ssh.close()
			s.close()

	print 'Completed transfer'
	return None

if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	output_host = raw_input('Destination directory host: ')
	output_dir = raw_input('Destination directory: ')
	input_paths = delete_check(input_host)
	delete_files(input_host, input_paths, output_host, output_dir)
