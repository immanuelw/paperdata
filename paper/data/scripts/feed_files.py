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

def gen_feed_data(host, full_path):
	'''
	generates data for feed table

	input: system host, full path of uv* file
	output: dict of feed table fields, log dict
	'''
	#allows uv access
	try:
		uv = A.miriad.UV(full_path)
	except:
		return None

	timestamp = int(time.time())

	feed_data = {'host':host,
				'path':os.path.dirname(full_path)
				'filename':os.path.basename(full_path)
				'full_path':full_path,
				'julian_day':int(uv['time']),
				'ready_to_move':False,
				'moved_to_distill':False,
				'timestamp':timestamp}

	log_data = {'action':'add by feed',
				'table':'feed',
				'identifier':full_path,
				'timestamp':timestamp}

	return feed_data, log_data

def dupe_check(input_host, input_paths):
	'''
	checks for files already in feed table

	input: file host, list of file paths
	output: list of files not in feed table
	'''
	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Feed')
		FEEDs = s.query(table).all()
	#all files on same host
	filenames = tuple(os.path.join(getattr(FEED, 'path'), getattr(FEED, 'filename')) for FEED in FEEDs if getattr(FEED, 'host') == input_host)

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	return unique_paths

def add_feeds_to_db(input_host, input_paths):
	'''
	adds feed file data to table

	input: file host, list of file paths
	'''
	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		for source in input_paths:
			feed_data, log_data = gen_feed_data(input_host, source)
			dbi.add_to_table(s, 'feed', feed_data)
			dbi.add_to_table(s, 'log', log_data)

	return None

if __name__ == '__main__':
	named_host = socket.gethostname()
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

	if named_host == input_host:
		input_paths = glob.glob(input_paths)
	else:
		with ppdata.ssh_scope(host) as ssh:
			input_paths = raw_input('Source directory path: ')
			_, path_out, _ = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
			input_paths = path_out.read().split('\n')[:-1]

	output_host = 'folio'
	feed_output = '/data4/paper/feed/'
	input_paths = dupe_check(input_host, input_paths)
	add_feeds_to_db(input_host, input_paths)
