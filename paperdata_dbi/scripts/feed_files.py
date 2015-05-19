#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import sys
import os
import glob
import socket
import aipy as A
import paperdata_dbi

### Script to load data from anywhere into paperfeed database
### Crawls folio or elsewhere and reads through .uv files to generate all field information

### Author: Immanuel Washington
### Date: 05-18-14

def gen_feed_data(host, full_path):
	#mostly file data
	host = host
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)

	#allows uv access
	try:
		uv = A.miriad.UV(full_path)
	except:
		return None

	#indicates julian date
	julian_date = round(uv['time'], 5)
	julian_day = int(str(julian_date)[3:7])

	ready_to_move = False
	moved_to_distill = False

	feed_data = (host, path, filename, full_path, julian_day, ready_to_move, moved_to_distill)

	return feed_data

def dupe_check(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FEEDs = s.query(dbi.Feed).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FEED.path, FEED.filename) for FEED in FEEDs if FEED.host==input_host)

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	return unique_paths

def add_feeds(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	for input_path in input_paths:
		path = os.path.dirname(input_path)
		filename = os.path.basename(input_path)
		feed_data = gen_feed_data(input_host, path, filename)
		dbi.add_observation(*feed_data)

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
	add_feeds(input_host, input_paths)
