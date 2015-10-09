#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import sys
import os
import time
import glob
import socket
import aipy as A
import paper as ppdata
from paper.data import dbi as pdbi

### Script to load data from anywhere into paperfeed database
### Crawls folio or elsewhere and reads through .uv files to generate all field information
### DOES NOT MOVE ANY DATA

### Author: Immanuel Washington
### Date: 05-18-14

def gen_feed_data(host, path):
	'''
	generates data for feed table

	Parameters
	----------
	host | str: system host
	path | str: path of uv* file

	Returns
	-------
	tuple:
		dict: feed values
		dict: log values
	'''
	#allows uv access
	try:
		uv = A.miriad.UV(path)
	except:
		return (None,) * 2

	base_path, filename = os.path.split(path)
	full_path = ':'.join((host, path))

	timestamp = int(time.time())

	feed_data = {'host': host,
				'basE_path': base_path,
				'filename': filename,
				'full_path': full_path,
				'julian_day': int(uv['time']),
				'is_movable': False,
				'is_moved': False,
				'timestamp': timestamp}

	log_data = {'action': 'add by feed',
				'table': 'Feed',
				'identifier': full_path,
				'timestamp': timestamp}

	return feed_data, log_data

def dupe_check(dbi, input_host, input_paths):
	'''
	checks for files already in feed table on the same host

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: file host
	input_paths | list[str]: file paths

	Returns
	-------
	list[str]: files not in feed table
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Feed')
		FEEDs = s.query(table).filter(getattr(table, 'host') == input_host).all()
	#all files on same host
	all_paths = tuple(os.path.join(getattr(FEED, 'base_path'), os.path.join(FEED, 'filename')) for FEED in FEEDs)

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in all_paths)
		
	return unique_paths

def add_feeds_to_db(dbi, input_host, input_paths):
	'''
	adds feed file data to table

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: file host
	input_paths | list[str]: file paths
	'''
	with dbi.session_scope() as s:
		for source in input_paths:
			feed_data, log_data = gen_feed_data(input_host, source)
			dbi.add_entry_dict(s, 'Feed', feed_data)
			dbi.add_entry_dict(s, 'Log', log_data)

def add_feeds(dbi, input_host, input_paths):
	'''
	generates list of input files, check for duplicates, add information to database

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: file host
	input_paths | str: file paths string
	'''
	named_host = socket.gethostname()
	if named_host == input_host:
		input_paths = glob.glob(input_paths)
	else:
		with ppdata.ssh_scope(input_host) as ssh:
			input_paths = raw_input('Source directory path: ')
			_, path_out, _ = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
			input_paths = path_out.read().split('\n')[:-1]

	output_host = 'folio'
	feed_output = '/data4/paper/feed/'
	input_paths = dupe_check(dbi, input_host, input_paths)
	add_feeds_to_db(dbi, input_host, input_paths)

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
	add_feeds(dbi, input_host, input_paths)
