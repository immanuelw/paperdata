#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to pyganglia

import MySQLdb
import sys
import getpass
import time
import csv
import subprocess
import glob
import socket
import os
import paramiko
import pyganglia_dbi

### Script to add info to pyganglia database
### Adds information using dbi

### Author: Immanuel Washington
### Date: 5-06-15

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(host, username=username, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
	except:
		try:
			ssh.connect(host, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
		except:
			return None

	return ssh


def calc_size(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	size = 0
	if named_host == host:
		size = round(float(sizeof_fmt(get_size(full_path))), 1)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		size_bytes = sftp.stat(full_path).st_size
		size = round(float(sizeof_fmt(size_bytes)), 1)
		sftp.close()
		ssh.close()

	return size

def add_data(input_host, input_paths):
	dbi = pyganglia_dbi.DataBaseInterface()
	for input_path in input_paths:
		path = os.path.dirname(input_path)
		filename = os.path.basename(input_path)
		obs_data, file_data = calc_uv_data(input_host, path, filename)
		dbi.add_observation(*obs_data)
		dbi.add_file(*file_data)

	return None

if __name__ == '__main__':
	if len(sys.argv) == 2:
		input_host = sys.argv[1].split(':')[0]
		if input_host == sys.argv[1]:
			print 'Needs host'
			sys.exit()
		input_paths = glob.glob(sys.argv[1].split(':')[1])
	elif len(sys.argv) == 3:
		input_host = sys.argv[1]
		input_paths = glob.glob(sys.argv[2])
	else:
		input_host = raw_input('Source directory host: ')
		input_paths = glob.glob(raw_input('Source directory path: '))

	input_paths = dupe_check(input_host, input_paths)
	input_paths.sort()
	npz_paths = [npz_path for npz_path in input_paths if '.npz' in npz_path]
	npz_paths.sort()
	input_paths = [input_path for input_path in input_paths if '.npz' not in input_path]
	input_paths.sort()
	add_files(input_host, input_paths)
	add_files(input_host, npz_paths)
	update_obsnums()
