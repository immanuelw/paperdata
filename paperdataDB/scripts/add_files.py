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
def login_ssh(host, username=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(host, key_filename='~/.ssh/id_rsa')
	except:
		try:
			ssh.connect(host, username=username, key_filename='~/.ssh/id_rsa')
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

#create function to uniquely identify files
def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = A.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

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

	return md5

def calc_obs_data(host, full_path):
	#mostly file data
	host = host
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)
	filetype = filename.split('.')[-1]

	#Dictionary of polarizations
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

	#allows uv access
	if filetype in ('uv', 'uvcRRE'):
		try:
			uv = A.miriad.UV(full_path)
		except:
			return None

		#indicates julian date
		julian_date = round(uv['time'], 5)

		#assign letters to each polarization
		if uv['npol'] == 1:
			polarization = pol_dict[uv['pol']]
		elif uv['npol'] == 4:
			polarization = 'all'

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
			return None

		if n_times > 1:
			delta_time = -(time_start - time_end)/(n_times - 1)
		else:
			delta_time = -(time_start - time_end)/(n_times)

		length = round(n_times * delta_time, 5)

		#gives each file unique id
		if length > 0:
			obsnum = jdpol2obsnum(julian_date, polarization, length)
		else:
			obsnum = 0

	elif filetype in ('npz',):
		#filename is zen.2456640.24456.xx.uvcRE.npz or zen.2456243.24456.uvcRE.npz
		jdate = filename.split('.')[1] + '.' + filename.split('.')[2]
		julian_date = round(float(jdate, 5)

		dbi = paperdata_dbi.DataBaseInterface()
		s = dbi.Session()
		if len(filename.split('.')) == 5:
			polarization = 'all'
			pol = polarization
		elif len(filename.split('.')) == 6:
			polarization = filename.split('.')[3]
			pol = polarization
		OBS = s.query(dbi.Observation).filter(dbi.Observation.julian_date==julian_date).filter(dbi.Observation.polarization==pol).one()

		time_start = OBS.time_start		
		time_end = OBS.time_end
		delta_time_start = OBS.delta_time		
		length = OBS.length
		obsnum = OBS.obsnum		

		s.close()

	#indicates julian day and set of data
	if julian_date < 2456100:
		era = 32
	elif julian_date < 2456400:
		era = 64
	else:
		era = 128

	julian_day = int(str(julian_date)[3:7])

	#indicates type of file in era
	era_type = 'NULL'

	#location of calibrate files
	#if era == 32:
	#	cal_path = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
	#elif era == 64:
	#	cal_path = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'
	#elif era == 128:
	#	cal_path = 'NULL'

	#unknown prev/next observation
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	PREV_OBS = s.query(dbi.Observation).filter(dbi.Observation.obsnum==obsnum-1).one()
	if PREV_OBS is not None:
		prev_obs = PREV_OBS.obsnum
	else:
		prev_obs = None
	NEXT_OBS = s.query(dbi.Observation).filter(dbi.Observation.obsnum==obsnum+1).one()
	if NEXT_OBS is not None:
		next_obs = NEXT_OBS.obsnum
	else:
		next_obs = None
	s.close()
	edge = (None in (prev_obs, next_obs))

	filesize = calc_size(host, path, filename)
	md5 = calc_md5sum(host, path, filename)
	tape_index = 'NULL'

	write_to_tape = 0
	delete_file = 0

	obs_data = (obsnum, julian_date, polarization, julian_day, era, era_type, length, time_start, time_end, delta_time, prev_obs, next_obs, edge)
	file_data = (host, path, filename, filetype, obsnum, filesize, md5, tape_index, write_to_tape, delete_file) #cal_path?? XXXX

	return obs_data, file_data

def calc_uv_data(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)

	if named_host == host:
		obs_data, file_data = calc_obs_data(host, full_path)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		#allows uv access
		#XXXX DO NOT KNOW IF THIS WORKS -- HOW TO UV REMOTE FILE??
		remote_path = sftp.file(full_path, mode='r')
		obs_data, file_data = calc_obs_data(host, remote_path)

	return obs_data, file_data

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

def update_obsnums():
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(dbi.Observation).all()
	s.close()
	for OBS in OBSs:
		PREV_OBS = s.query(dbi.Observation).filter(dbi.Observation.obsnum==OBS.obsnum-1).one()
		if PREV_OBS is not None:
			prev_obs = PREV_OBS.obsnum
			dbi.set_prev_obs(OBS.obsnum, prev_obs)
		else:
			prev_time = OBS.time_start - OBS.delta_time
			pol = OBS.polarization
			PREV_OBS = s.query(dbi.Observation).filter(dbi.Observation.julian_date==prev_time).filter(dbi.Observation.polarization==pol).one()
			if PREV_OBS is not None:
				prev_obs = PREV_OBS.obsnum
				dbi.set_prev_obs(OBS.obsnum, prev_obs)

		NEXT_OBS = s.query(dbi.Observation).filter(dbi.Observation.obsnum==OBS.obsnum+1).one()
		if NEXT_OBS is not None:
			next_obs = NEXT_OBS.obsnum
			dbi.set_next_obs(OBS.obsnum, next_obs)
		else:
			next_time = OBS.time_end + OBS.delta_time
			pol = OBS.polarization
			NEXT_OBS = s.query(dbi.Observation).filter(dbi.Observation.julian_date==next_time).filter(dbi.Observation.polarization==pol).one()
			if NEXT_OBS is not None:
				next_obs = NEXT_OBS.obsnum
				dbi.set_next_obs(OBS.obsnum, next_obs)

		#sets edge 
		dbi.set_edge(OBS.obsnum, edge=(None in (PREV_OBS, NEXT_OBS)))

	return None

def add_files(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
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

	input_paths.sort()
	dupes = dupe_check(input_host, input_paths)
	if not dupes:
		#if any copies, don't load anything
		print 'Duplicate found'
		sys.exit()
	npz_paths = [npz_path for npz_path in input_paths if '.npz' in npz_path]
	npz_paths.sort()
	input_paths = [input_path for input_path in input_paths if '.npz' not in input_path]
	input_paths.sort()
	add_files(input_host, input_paths)
	add_files(input_host, npz_paths)
	update_obsnums()
