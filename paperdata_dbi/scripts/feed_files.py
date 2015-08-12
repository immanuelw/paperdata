#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import sys
import os
import time
import glob
import socket
import aipy as A
import paperdata_dbi as pdbi

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

	timestamp = int(time.time())

	feed_data = {'host':host,
				'path':path,
				'filename':filename,
				'full_path':full_path,
				'julian_day':julian_day,
				'ready_to_move':ready_to_move,
				'moved_to_distill':moved_to_distill,
				'timestamp':timestamp}

	action = 'add by feed'
	table = 'feed'
	log_data = {'action':action,
				'table':table,
				'obsnum':None,
				'host':host,
				'full_path':None,
				'feed_path':full_path,
				'timestamp':timestamp}

	return feed_data, log_data

def rsync_copy(source, destination):
	subprocess.check_output(['rsync', '-ac', source, destination])
	return None

def move_files(input_host, input_paths, output_host, output_dir):
	#different from move_files, adds to feed
	named_host = socket.gethostname()
	destination = ''.join((output_host, ':', output_dir))
	if named_host == input_host:
		dbi = pdbi.DataBaseInterface()
		s = dbi.Session()
		for source in input_paths:
			rsync_copy(source, destination)
			add_feed_to_db(input_host, source)
			shutil.rmtree(source)
		s.close()
	else:
		dbi = pdbi.DataBaseInterface()
		s = dbi.Session()
		ssh = pdbi.login_ssh(output_host)
		for source in input_paths:
			rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
			rsync_del_command = '''rm -r {source}'''.format(source=source)
			ssh.exec_command(rsync_copy_command)
			add_feed_to_db(input_host, source)
			ssh.exec_command(rsync_del_command)
		ssh.close()
		s.close()

	print 'Completed transfer'
	return None

def dupe_check(input_host, input_paths):
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	FEEDs = s.query(pdbi.Feed).all()
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FEED.path, FEED.filename) for FEED in FEEDs if FEED.host==input_host)

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	return unique_paths

def add_feed_to_db(input_host, full_path):
	dbi = pdbi.DataBaseInterface()
	feed_data, log_data = gen_feed_data(input_host, full_path)
	dbi.add_feed(feed_data)
	dbi.add_log(log_data)

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

	output_host = 'folio'
	feed_output = '/data4/paper/feed/'
	input_paths = dupe_check(input_host, input_paths)
	input_paths = move_files(input_host, input_paths, output_host, feed_output)
	input_paths.sort()
	add_feeds(input_host, input_paths)
