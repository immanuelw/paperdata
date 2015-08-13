#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdev

import sys
import aipy as A
import hashlib
import glob
import socket
import os
import paramiko
import paperdev_dbi as pdbi
import time
import uv_data

### Script to add files to paperdev database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

#Functions which simply find the file size
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	#converts bytes to MB
	for byte_size in ('KB', 'MB'):
		num /= 1024.0
	return round(num, 1)

### other functions
def calc_size(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	size = 0
	if named_host == host:
		size = sizeof_fmt(get_size(full_path))
	else:
		ssh = pdbi.login_ssh(host)
		sftp = ssh.open_sftp()
		size_bytes = sftp.stat(full_path).st_size
		size = sizeof_fmt(size_bytes)
		sftp.close()
		ssh.close()

	return size

def calc_md5sum(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	md5 = None
	if named_host == host:
		md5 = pdbi.get_md5sum(full_path)
	else:
		ssh = pdbi.login_ssh(host)
		try:
			sftp = ssh.open_sftp()
			remote_path = sftp.file(full_path, mode='r')
			md5 = remote_path.check('md5', block_size=65536)
			sftp.close()
		except(IOError):
			stdin, md5_out, stderr = ssh.exec_command('md5sum {full_path}/visdata'.format(full_path=full_path))		
			md5 = md5_out.read().split(' ')[0]
		ssh.close()

	return md5

def get_uv_data(host, full_path, mode=None):
	ssh = pdbi.login_ssh(host)
	uv_data_script = os.path.expanduser('~/paperdev/paperdev_dbi/scripts/uv_data.py')
	sftp = ssh.open_sftp()
	moved_script = './uv_data.py'
	try:
		filestat = sftp.stat(uv_data_script)
	except(IOError):
		try:
			filestat = sftp.stat(moved_script)
		except(IOError):
			sftp.put(uv_data_script, moved_script)
	sftp.close()

	if mode is None:
		uv_comm = 'python {moved_script} {host} {full_path}'.format(moved_script=moved_script, host=host, full_path=full_path))
		_, uv_dat, _ = ssh.exec_command(uv_comm)
		time_start, time_end, delta_time, julian_date, polarization, length, obsnum = [round(float(info), 5) if key in (0, 1, 2, 3, 5)
																						else int(info) if key in (6,)
																						else info
																						for key, info in enumerate(uv_dat.read().split(','))]
		ssh.close()
		return time_start, time_end, delta_time, julian_date, polarization, length, obsnum

	elif mode =='time':
		uv_comm = 'python {moved_script} {host} {full_path} time'.format(moved_script=moved_script, host=host, full_path=full_path)
		_, uv_dat, _ = ssh.exec_command(uv_comm)
		time_start, time_end, delta_time, length = [round(float(info), 5) for info in uv_dat.read().split(',')]
		ssh.close()
		return time_start, time_end, delta_time, length

def julian_era(julian_date):
	#indicates julian day and set of data
	if julian_date < 2456100:
		era = 32
	elif julian_date < 2456400:
		era = 64
	else:
		era = 128

	julian_day = int(str(julian_date)[3:7])

	return era, julian_day

def obs_pn(s):
	PREV_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.obsnum==obsnum-1).one()
	if PREV_OBS is not None:
		prev_obs = PREV_OBS.obsnum
	else:
		prev_obs = None
	NEXT_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.obsnum==obsnum+1).one()
	if NEXT_OBS is not None:
		next_obs = NEXT_OBS.obsnum
	else:
		next_obs = None

	return prev_obs, next_obs

def obs_edge(obsnum, sess=None):
	#unknown prev/next observation
	if obsnum == None:
		prev_obs = None
		next_obs = None
	else:
		if sess is None:
			dbi = pdbi.DataBaseInterface()
			s = dbi.Session()
			prev_obs, next_obs = obs_pn(s)
			s.close()
		else:
			prev_obs, next_obs = obs_pn(sess)

	if (prev_obs, next_obs) == (None, None):
		edge = None
	else:
		edge = (None in (prev_obs, next_obs))

	return prev_obs, next_obs, edge

def file_names(full_path)
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)
	filetype = filename.split('.')[-1]

	return path, filename, filetype

def calc_obs_data(host, full_path):
	#mostly file data
	host = host
	path, filename, filetype = file_names(full_path)

	#Dictionary of polarizations
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

	#allows uv access
	named_host = socket.gethostname()
	if filetype in ('uv', 'uvcRRE'):
		if named_host == host:
			time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_uv_data(host, full_path)
		else:
			time_start, time_end, delta_time, julian_date, polarization, length, obsnum = get_uv_data(host, full_path)

	elif filetype in ('npz',):
		#filename is zen.2456640.24456.xx.uvcRE.npz or zen.2456243.24456.uvcRE.npz
		jdate = ''.join(filename.split('.')[1], '.', filename.split('.')[2])
		julian_date = round(float(jdate, 5))

		dbi = pdbi.DataBaseInterface()
		s = dbi.Session()
		if len(filename.split('.')) == 5:
			polarization = 'all'
			pol = polarization
		elif len(filename.split('.')) == 6:
			polarization = filename.split('.')[3]
			pol = polarization
		OBS = s.query(pdbi.Observation).filter(pdbi.Observation.julian_date==julian_date).filter(pdbi.Observation.polarization==pol).one()

		time_start = OBS.time_start		
		time_end = OBS.time_end
		delta_time_start = OBS.delta_time		
		length = OBS.length
		obsnum = OBS.obsnum		

		s.close()

	era, julian_day = julian_era(julian_date)

	#indicates type of file in era
	era_type = None

	prev_obs, next_obs, edge = obs_edge(obsnum)

	filesize = calc_size(host, path, filename)
	md5 = calc_md5sum(host, path, filename)
	tape_index = None

	source_host = host
	write_to_tape = False
	delete_file = False

	timestamp = int(time.time())

	obs_data = {'obsnum':obsnum,
				'julian_date':julian_date,
				'polarization':polarization,
				'julian_day':julian_day,
				'era':era,
				'era_type':era_type,
				'length':length,
				'time_start':time_start,
				'time_end':time_end,
				'delta_time':delta_time,
				'prev_obs':prev_obs, 
				'next_obs':next_obs,
				'edge':edge,
				'timestamp':timestamp}
	file_data = {'host':host,
				'path':path,
				'filename':filename,
				'filetype':filetype,
				'full_path':full_path,
				'obsnum':obsnum,
				'filesize':filesize,
				'md5sum':md5,
				'tape_index':tape_index,
				'source_host':source_host,
				'write_to_tape':write_to_tape,
				'delete_file':delete_file,
				'timestamp':timestamp}

	action = 'add by scan'
	table = None
	log_data = {'action':action,
				'table':table,
				'obsnum':obsnum,
				'host':host,
				'full_path':full_path,
				'feed_path':None,
				'timestamp':timestamp}

	return obs_data, file_data, log_data

def dupe_check(input_host, input_paths):
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(pdbi.File).all()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs if FILE.host == input_host)

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
	s.close()
		
	return unique_paths

def obsnum_list(obsnum):
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(pdbi.Observation).all()
	s.close()
	#all files on same host
	obsnums = tuple(OBS.obsnum for OBS in OBSs)

	return obsnums

def update_obsnums():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(pdbi.Observation).all()
	s.close()
	for OBS in OBSs:
		PREV_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.obsnum==OBS.obsnum-1).one()
		if PREV_OBS is not None:
			prev_obs = PREV_OBS.obsnum
			dbi.set_prev_obs(OBS.obsnum, prev_obs)
		else:
			prev_time = OBS.time_start - OBS.delta_time
			pol = OBS.polarization
			PREV_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.julian_date==prev_time).filter(pdbi.Observation.polarization==pol).one()
			if PREV_OBS is not None:
				prev_obs = PREV_OBS.obsnum
				dbi.set_prev_obs(OBS.obsnum, prev_obs)

		NEXT_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.obsnum==OBS.obsnum+1).one()
		if NEXT_OBS is not None:
			next_obs = NEXT_OBS.obsnum
			dbi.set_next_obs(OBS.obsnum, next_obs)
		else:
			next_time = OBS.time_end + OBS.delta_time
			pol = OBS.polarization
			NEXT_OBS = s.query(pdbi.Observation).filter(pdbi.Observation.julian_date==next_time).filter(pdbi.Observation.polarization==pol).one()
			if NEXT_OBS is not None:
				next_obs = NEXT_OBS.obsnum
				dbi.set_next_obs(OBS.obsnum, next_obs)

		#sets edge 
		dbi.set_edge(OBS.obsnum, edge=(None in (PREV_OBS, NEXT_OBS)))

	return None

def add_files(input_host, input_paths):
	dbi = pdbi.DataBaseInterface()
	for input_path in input_paths:
		path = os.path.dirname(input_path)
		filename = os.path.basename(input_path)
		obs_data, file_data, log_data = calc_obs_data(input_host, input_path)
		try:
			dbi.add_observation(obs_data)
		except:
			print('Failed to load in obs ', path, filename)
		try:
			dbi.add_file(file_data)
		except:
			print('Failed to load in file ', path, filename)
		try:
			dbi.add_log(log_data)
		except:
			print('Failed to load in log ', path, filename)

	return None

if __name__ == '__main__':
	named_host = socket.gethostname()
	if len(sys.argv) == 2:
		input_host = sys.argv[1].split(':')[0]
		if input_host == sys.argv[1]:
			print 'Needs host'
			sys.exit()
		input_paths = sys.argv[1].split(':')[1]
	elif len(sys.argv) == 3:
		input_host = sys.argv[1]
		input_paths = sys.argv[2]
	else:
		input_host = raw_input('Source directory host: ')
		input_paths = raw_input('Source directory path: ')

	if named_host == input_host:
		input_paths = glob.glob(input_paths)
	else:
		ssh = pdbi.login_ssh(input_host)
		input_paths = raw_input('Source directory path: ')
		stdin, path_out, stderr = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
		input_paths = path_out.read().split('\n')[:-1]
		ssh.close()

	input_paths = dupe_check(input_host, input_paths)
	input_paths.sort()
	npz_paths = [npz_path for npz_path in input_paths if '.npz' in npz_path]
	npz_paths.sort()
	input_paths = [input_path for input_path in input_paths if '.npz' not in input_path]
	input_paths.sort()
	add_files(input_host, input_paths)
	add_files(input_host, npz_paths)
	update_obsnums()
