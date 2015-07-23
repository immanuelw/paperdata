#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import sys
import os
import glob
import socket
import aipy as A
import paperdata_dbi as pdbi
import move_files

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

def rsync_m(source, destination):
	subprocess.check_output(['rsync', '-a', source, destination])
	return None

def move_files(input_host, input_paths, output_host, output_dir):
	named_host = socket.gethostname()
	destination = output_host + ':' + output_dir
	input_filenames = tuple(os.path.basename(input_path) for input_path in input_paths)
	moved_files = tuple(os.path.join(destination, input_filename) for input_file in input_filenames)
	if named_host == input_host:
		if input_host == output_host:
			for source in input_paths:
				shutil.move(source, output_dir)
		else:
			for source in input_paths:
				rsync_m(source, destination)
	else:
		if input_host == output_host:
			dbi = pdbi.DataBaseInterface()
			s = dbi.Session()
			ssh = pdbi.login_ssh(output_host)
			sftp = ssh.open_sftp()
			for source in input_paths:
				sftp.rename(source, output_dir)
			sftp.close()
			ssh.close()
			s.close()
		else:
			dbi = pdbi.DataBaseInterface()
			s = dbi.Session()
			ssh = pdbi.login_ssh(output_host)
			for source in input_paths:
				rsync_move = '''rsync -a {source} {destination}'''.format(source=source, destination=destination)
				ssh.exec_command(rsync_move)
			ssh.close()
			s.close()

	print 'Completed transfer'
	return moved_files

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

def add_feeds(input_host, input_paths):
	dbi = pdbi.DataBaseInterface()
	for input_path in input_paths:
		path = os.path.dirname(input_path)
		filename = os.path.basename(input_path)
		feed_data = gen_feed_data(input_host, path, filename)
		dbi.add_observation(*feed_data)

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
		stdin, path_out, stderr = ssh.exec_command('ls -d {0}'.format(input_paths))
		input_paths = path_out.read().split('\n')[:-1]
		ssh.close()

	output_host = 'folio'
	feed_output = '/data4/paper/feed/'
	input_paths = dupe_check(input_host, input_paths)
	input_paths = move_files(input_host, input_paths, output_host, feed_output)
	input_paths.sort()
	add_feeds(input_host, input_paths)
