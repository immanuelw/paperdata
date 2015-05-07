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

### Script to add files to paperdata database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username=None, password=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(host)
	except:
		try:
			ssh.connect(host, username=username, password=password)
		except:
			return None

	return ssh

#Functions which simply find the file size
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['bytes','KB','MB']:
		if num < 1024.0:
			return "%3.1f" % (num)
		num /= 1024.0
	num *= 1024.0
	return "%3.1f" % (num)

### other functions

def md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(fname, 'rb')
	except(IOError):
		afile = open("%s/visdata"%fname, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)
	return hasher.hexdigest()

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

def calc_md5sum(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	md5 = 'NULL'
	if named_host == host:
		md5 = md5sum(full_path)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		remote_path = sftp.file(full_path, mode='r')
		md5 = remote_path.check('md5', block_size=65536)
		sftp.close()
		ssh.close()

def calc_uv_data(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	times =  ('NULL', 'NULL', 'NULL')
	#Dictionary of polarizations
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

	if named_host == host:
		#allows uv access
		try:
			uv = A.miriad.UV(full_path)
		except:
			return times

		#indicates julian date
		jdate = round(uv['time'], 5)

		#indicates julian day and set of data
		if jdate < 2456100:
			era = 32
		elif jdate < 2456400:
			era = 64
		else:
			era = 128

		jday = int(str(jdate)[3:7])

		#indicates type of file in era
		era_type = 'NULL'

		#assign letters to each polarization
		if uv['npol'] == 1:
			polarization = pol_dict[uv['pol']]
		elif uv['npol'] == 4:
		#	polarization = 'all' #default to 'yy' as 'all' is not a key for jdpol2obsnum
			polarization = 'yy'

		time_start = 0
		time_end = 0
		n_times = 0
		c_time = 0

		try:
			for (uvw, t, (i,j)),d in uv.all():
				if time_start == 0 or t < time_start:
					time_start = t
				if time_end == 0 or t > time_end:
					time_end = t
				if c_time != t:
					c_time = t
					n_times += 1
		except:
			return times

		if n_times > 1:
			delta_time = -(time_start - time_end)/(n_times - 1)
		else:
			delta_time = -(time_start - time_end)/(n_times)

		times = (time_start, time_end, delta_time)

		length = round(n_times * delta_time, 5)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		#allows uv access
		#XXXX DO NOT KNOW IF THIS WORKS -- HOW TO UV REMOTE FILE??
		remote_path = sftp.file(full_path, mode='r')
		try:
			uv = A.miriad.UV(remote_path)
		except:
			return times

		time_start = 0
		time_end = 0
		n_times = 0
		c_time = 0

		try:
			for (uvw, t, (i,j)),d in uv.all():
				if time_start == 0 or t < time_start:
				time_start = t
				if time_end == 0 or t > time_end:
				time_end = t
				if c_time != t:
				c_time = t
				n_times += 1
		except:
			return times

		if n_times > 1:
			delta_time = -(time_start - time_end)/(n_times - 1)
		else:
			delta_time = -(time_start - time_end)/(n_times)

		times = (time_start, time_end, delta_time)

		sftp.close()
		ssh.close()

	return times

def dupe_check(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs if FILE.host == input_host)

	#for each input file, check if in filenames
	duplicates = tuple(input_path for input_path in input_paths if input_path in filenames)
		
	if len(duplicates) > 0:
		return False

	return True

def obsnum_list(obsnum):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(dbi.Observation).all()
	s.close()
	#all files on same host
	obsnums = tuple(OBS.obsnum for OBS in OBSs)

	return obsnums

def add_files(input_host, input_path)


if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	input_paths = glob.glob(raw_input('Source directory path: '))
	input_paths.sort()
	dupes = dupe_check(input_host, input_paths)
	if not dupes:
		#if any copies, don't load anything
		print 'Duplicate found'
		sys.exit()
	add_files(input_host, input_path)
