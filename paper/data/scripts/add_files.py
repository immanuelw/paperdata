#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paper

from __future__ import print_function
import os
import sys
import glob
import time
import socket
import paper as ppdata
from paper.data import dbi as pdbi, uv_data, file_data
from sqlalchemy import or_

### Script to add files to paper database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def calc_obs_data(dbi, host, full_path):
	'''
	generates all relevant data from uv* file

	Parameters
	----------
	host (str): host of system
	full_path (str): full path of uv* file

	Returns
	-------
	tuple:
		dict: observation values
		dict: file values
		dict: log values
	'''
	path, filename, filetype = file_data.file_names(full_path)

	if filetype in ('uv', 'uvcRRE'):
		time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_uv_data(host, full_path)
	elif filetype in ('npz',):
		time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_npz_data(dbi, filename)

	era, julian_day, lst = uv_data.date_info(julian_date)

	#indicates type of file in era
	era_type = None

	prev_obs = None
	next_obs = None
	is_edge = None

	filesize = file_data.calc_size(host, path, filename)
	md5 = file_data.calc_md5sum(host, path, filename)
	tape_index = None

	source_host = host
	is_tapeable = False
	is_deletable = False

	timestamp = int(time.time())

	obs_data = {'obsnum': obsnum,
				'julian_date': julian_date,
				'polarization': polarization,
				'julian_day': julian_day,
				'lst': lst,
				'era': era,
				'era_type': era_type,
				'length': length,
				'time_start': time_start,
				'time_end': time_end,
				'delta_time': delta_time,
				'prev_obs': prev_obs, 
				'next_obs': next_obs,
				'is_edge': is_edge,
				'timestamp': timestamp}
	file_data = {'host': host,
				'path': path,
				'filename': filename,
				'filetype': filetype,
				'full_path': full_path,
				'obsnum': obsnum,
				'filesize': filesize,
				'md5sum': md5,
				'tape_index': tape_index,
				'source_host': source_host,
				'is_tapeable': is_tapeable,
				'is_deletable': is_deletable,
				'timestamp': timestamp}

	log_data = {'action': 'add by scan',
				'table': None,
				'identifier': full_path,
				'timestamp': timestamp}

	return obs_data, file_data, log_data

def dupe_check(dbi, input_host, input_paths):
	'''
	checks for duplicate paths and removes to not waste time if possible

	Parameters
	----------
	dbi (object): database interface object
	input_host (str): host of uv* files
	input_paths (list): paths of uv* files

	Returns
	-------
	list: paths that are not already in database
	'''
	with dbi.session_scope() as s:
		#all files on same host
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'host') == input_host).all()
		full_paths = tuple(os.path.join(getattr(FILE, 'path'), getattr(FILE, 'filename')) for FILE in FILEs)

	#for each input file, check if in full_paths
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in full_paths)
		
	return unique_paths

def set_obs(s, dbi, OBS, field):
	'''
	finds edge observation for each observation by finding previous and next

	Parameters
	----------
	s (object): session object
	dbi (object): database interface object
	OBS (object): observation object
	field (str): field to update

	Returns
	-------
	object: edge observation object
	'''
	if field == 'prev_obs':
		edge_num = getattr(OBS, 'obsnum') - 1
		edge_time = getattr(OBS, 'time_start') - getattr(OBS, 'delta_time')
	elif field == 'next_obs':
		edge_num = getattr(OBS, 'obsnum') + 1
		edge_time = getattr(OBS, 'time_start') + getattr(OBS, 'delta_time')

	table = getattr(pdbi, 'Observation')
	EDGE_OBS = s.query(table).filter(getattr(table, 'julian_date') == edge_time).one()
	if EDGE_OBS is not None:
		edge_obs = getattr(EDGE_OBS, 'obsnum')
		dbi.set_entry(s, OBS, field, edge_obs)
	else:
		pol = getattr(OBS, 'polarization')
		EDGE_OBS = s.query(table).filter(getattr(table, 'julian_date') == edge_time).filter(getattr(table, 'polarization') == pol).one()
		if EDGE_OBS is not None:
			edge_obs = EDGE_OBS.obsnum
			dbi.set_entry(s, OBS, field, edge_obs)

	return EDGE_OBS

def update_obsnums(dbi):
	'''
	updates edge attribute of all obsnums

	Parameters
	----------
	dbi (object): database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Observation')
		OBSs = s.query(table).filter(or_(getattr(table, 'prev_obs') == None, getattr(table, 'next_obs') == None)).all()

		for OBS in OBSs:
			PREV_OBS = set_obs(s, dbi, OBS, 'prev_obs')
			NEXT_OBS = set_obs(s, dbi, OBS, 'next_obs')
			#sets edge 
			is_edge = uv_data.is_edge(PREV_OBS, NEXT_OBS)
			dbi.set_entry(s, OBS, 'is_edge', is_edge)

	return None

def connect_observations(dbi):
	'''
	connects file with observation object

	Parameters
	----------
	dbi (object): database interface object
	'''
	with dbi.session_scope() as s:
		file_table = getattr(pdbi, 'File')
		obs_table = getattr(pdbi, 'Observation')
		FILEs = s.query(file_table).filter(getattr(file_table, 'observation') == None).all()

		for FILE in FILEs:		
			#get the observation corresponding to this file
			OBS = s.query(obs_table).get(getattr(FILE, 'obsnum'))
			dbi.set_entry(s, FILE, 'observation', OBS)  #associate the file with an observation

	return None

def add_files_to_db(dbi, input_host, input_paths):
	'''
	adds files to the database

	Parameters
	----------
	dbi (object): database interface object
	input_host (str): host of files
	input_paths (list): paths of uv* files
	'''
	with dbi.session_scope() as s:
		for input_path in input_paths:
			path = os.path.dirname(input_path)
			filename = os.path.basename(input_path)
			obs_data, file_data, log_data = calc_obs_data(input_host, input_path)
			try:
				dbi.add_entry_dict(s, 'Observation', obs_data)
			except:
				print('Failed to load in obs ', path, filename)
			try:
				dbi.add_entry_dict(s, 'File', file_data)
			except:
				print('Failed to load in file ', path, filename)
			try:
				dbi.add_entry_dict(s, 'Log', log_data)
			except:
				print('Failed to load in log ', path, filename)

	return None

def add_files(dbi, input_host, input_paths):
	'''
	generates list of input files, check for duplicates, add information to database

	Parameters
	----------
	dbi (object): database interface object
	input_host (str): host of files, list of uv* file paths
	input_paths (str): string to indicate paths of uv* files
	'''
	named_host = socket.gethostname()
	if named_host == input_host:
		input_paths = glob.glob(input_paths)
	else:
		with ppdata.ssh_scope(input_host) as ssh:
			input_paths = raw_input('Source directory path: ')
			_, path_out, _ = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
			input_paths = path_out.read().split('\n')[:-1]

	input_paths = sorted(dupe_check(dbi, input_host, input_paths))
	npz_paths = [npz_path for npz_path in input_paths if npz_path.endswith('.npz')]
	input_paths = [input_path for input_path in input_paths if not input_path.endswith('.npz')]
	add_files_to_db(dbi, input_host, input_paths)
	add_files_to_db(dbi, input_host, npz_paths)
	update_obsnums(dbi)
	connect_observations(dbi)

	return None

if __name__ == '__main__':
	if len(sys.argv) == 2:
		input_host = sys.argv[1].split(':')[0]
		if input_host == sys.argv[1]:
			print('Needs host')
			sys.exit()
		input_paths = sys.argv[1].split(':')[1]
	elif len(sys.argv) == 3:
		input_host = sys.argv[1]
		input_paths = sys.argv[2]
	else:
		input_host = raw_input('Source directory host: ')
		input_paths = raw_input('Source directory path: ')

	dbi = pdbi.DataBaseInterface()
	add_files(dbi, input_host, input_paths)
