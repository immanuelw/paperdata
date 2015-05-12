#!/usr/bin/python
# -*- coding: utf-8 -*-

# import the MySQLdb and sys modules
import sys
import getpass
import os
import csv
import socket
import paperdata_dbi

def check_files(input_host):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILES = s.query(dbi.File).filter(dbi.File.host==input_host).filter(dbi.File.tape_index!=None).all()
	s.close()
	#tuple of path without host, and tuple of path and filename
	file_paths = tuple((os.path.join(FILE.path, FILE.filename), (FILE.path, FILE.filename)) for FILE in FILES)

	host = socket.gethostname()
	if host == input_host:
		for path in file_paths:
			if not os.path.isdir(path[0]) and not os.path.isfile(path[0]):
				delete_paths.append(path[1])
	else:
		ssh = paperdata_dbi.login_ssh(input_host)
		#check if files exist on host
		#return those who do not
		sftp = ssh.open_sftp()
		delete_paths = []
		for path in file_paths:
			try:
				if sftp.stat(path[0]):
					continue
			except IOError:
				delete_paths.append(path[1])
		sftp.close()
		ssh.close()

	delete_paths.sort()
	delete_paths = tuple(delete_paths)

	return delete_paths

def clean_db(input_host, delete_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	for path in delete_paths:
		FILE = s.query(dbi.File).filter(dbi.File.host==input_host).filter(dbi.File.path==path[0]).filter(dbi.File.filename==path[1]).one()
		s.delete(FILE)
	s.commit()
	s.close()

	return None

if __name__ == '__main__':
	if len(sys.argv) == 2:
		input_host = sys.argv[1]
	else:
		input_host = raw_input('Source directory host: ')
	delete_paths = check_files(input_host)
	clean_db(input_host, delete_paths)
