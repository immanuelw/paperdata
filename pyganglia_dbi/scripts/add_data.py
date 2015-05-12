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

def calculate_folio_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['df', '-B', '1'])
	#/data4 should be filesystem
	#Amount of available bytes should be free_space

	folio_space = 0
	for output in folio.split('\n'):
		filesystem = output.split(' ')[-1]
		if filesystem in ['/data4']:
			folio_space = int(output.split(' ')[-4])

	return folio_space

def iostat():
	#Calculates cpu usage on folio nodes
	folio = subprocess.check_output(['iostat'])

	folio_use = []
	folio_name = folio.split('\n')[0].split(' ')[2].strip('()')
	for output in folio.split('\n'):
		device = output.split(' ')[0]
		if device in ['Device:', 'sda', 'sda1', 'sda2','dm-0', 'dm-1']:
			line = output[:].split(' ')
			new_line = filter(lambda a: a not in [''], line)
			folio_use.append(new_line)

	device = ['Device:', 'sda', 'sda1', 'sda2','dm-0', 'dm-1']
	#Convert all numbers to floats, keep words as strings
	folio_use = [[float(i) if i not in device else i for i in j] for j in folio_use[1:]]

	return [folio_name, folio_use]

def ram_free():
	#Calculates ram usage on folio
	folio = subprocess.check_output(['free', '-b'])
	ram = []
	for output in folio.split('\n'):
		line = output[:].split(' ')
		new_line = filter(lambda a: a not in [''], line)
		ram.append(new_line)	

	reram = []
	for key, row in enumerate(ram[1:-1]):
		if key in [0,2]:
				reram.extend([int(i) for i in row[1:]])
		if key in [1]:
				reram.extend([int(i) for i in row[2:]])

	return [reram]

def cpu_perc():
	#Calculates cpu usage on folio
	folio = subprocess.check_output(['mpstat', '-P', 'ALL', '1', '1'])
	cpu = []
	for output in folio.split('\n'):
		if output in [folio.split('\n')[0],folio.split('\n')[1]]:
			continue
		line = output[:].split(' ')
		new_line = filter(lambda a: a not in [''], line)
		if new_line[0] not in ['Average:']:
			cpu.append(new_line)

	recpu = []
	for row in cpu[2:]:
		dummy_cpu = []
		for key, item in enumerate(row):
			if key in [2,3,5,6,10,11]:
				dummy_cpu.append(float(item))
		recpu.append(dummy_cpu)

	return recpu

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
	hosts = ('folio', 'node01', 'node02', 'node03', 'node04', 'node05', 'node06', 'node07', 'node08', 'node09', 'node10')

	for host in hosts:
		ssh = login_ssh(host)
		#run functions
		ssh.close()
