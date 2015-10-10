#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import os
import time
import shutil
import socket
import smtplib
import paper as ppdata
from paper.data import dbi as pdbi
import distill_files
import move_files
from sqlalchemy import func
from sqlalchemy.sql import label

### Script to load paperdistiller with files from the paperfeed table
### Checks /data4 for space, moves entire days of data, then loads into paperdistiller

### Author: Immanuel Washington
### Date: 11-23-14

def set_feed(s, dbi, source, output_host, output_dir, is_moved=True):
	'''
	updates table for feed file

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	source | str: source file
	output_host | str: output host
	output_dir | str: output directory
	is_moved | bool: checks whether to move to paperdistiller --defaults to False
	'''
	FEED = dbi.get_entry(s, 'Feed', source)
	dbi.set_entry(s, FEED, 'host', output_host)
	dbi.set_entry(s, FEED, 'base_path', output_dir)
	dbi.set_entry(s, FEED, 'is_moved', is_moved)

def move_feed_files(dbi, input_host, input_paths, output_host, output_dir):
	'''
	moves files and adds to feed directory and table

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: file host
	input_paths | list[str]: file paths
	source | str: source file
	output_host | str: output host
	output_dir | str: output directory
	'''
	#different from move_files, adds to feed
	named_host = socket.gethostname()
	destination = ':'.join((output_host, output_dir))
	with dbi.session_scope() as s:
		if named_host == input_host:
			for source in input_paths:
				ppdata.rsync_copy(source, destination)
				set_feed(s, dbi, source, output_host, output_dir)
				shutil.rmtree(source)
		else:
			with ppdata.ssh_scope(input_host) as ssh:
				for source in input_paths:
					rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
					rsync_del_command = '''rm -r {source}'''.format(source=source)
					ssh.exec_command(rsync_copy_command)
					set_feed(s, dbi, source, output_host, output_dir)
					ssh.exec_command(rsync_del_command)

	print('Completed transfer')

def count_days(dbi):
	'''
	checks amount of days in feed table and sets to move if reach requirement

	Parameters
	----------
	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Feed')
		count_FEEDs = s.query(getattr(table, 'julian_day'), label('count', func.count(getattr(table, 'julian_day'))))\
								.group_by(getattr(table, 'julian_day')).all()
		all_FEEDs = s.query(table).all()
		good_days = tuple(getattr(FEED, 'julian_day') for FEED in count_FEEDs if getattr(FEED, 'count') in (72, 288))
		to_move = tuple(os.path.join(getattr(FEED, 'base_path'), getattr(FEED, 'filename'))
						for FEED in all_FEEDs if getattr(FEED, 'julian_day') in good_days)

		for path in to_move:
			FEED = dbi.get_entry(s, 'Feed', source)
			dbi.set_entry(s, FEED, 'is_movable', True)

def find_data(dbi):
	'''
	finds data to move from feed table

	Parameters
	----------
	dbi | object: database interface object

	Returns
	-------
	tuple:
		list[str]: file paths to move
		str: file host
		list[str]: filenames to be moved
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Feed')
		FEEDs = s.query(table).filter(getattr(table, 'is_moved') == False).filter(getattr(table, 'is_movable') == True).all()

	#only move one day at a time
	feed_host = getattr(FEEDs[0], 'host')
	feed_day = getattr(FEEDs[0], 'julian_day')
	feed_paths = tuple(os.path.join(getattr(FEED, 'path'), getattr(FEED, 'filename'))
						for FEED in FEEDs if getattr(FEED, 'julian_day') == feed_day)
	feed_filenames = tuple(getattr(FEED, 'filename') for FEED in FEEDs if getattr(FEED, 'julian_day') == feed_day)

	return feed_paths, feed_host, feed_filenames

def feed_bridge(dbi):
	'''
	bridges feed and paperdistiller
	moves files and pulls relevant data to add to paperdistiller from feed

	Parameters
	----------
	dbi | object: database interface object
	'''
	#Minimum amount of space to move a day ~3.1TiB
	required_space = 1112373311360
	output_dir = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED

	#Move if there is enough free space
	if move_files.enough_space(required_space, output_dir):
		#check how many days are in each
		count_days(dbi)
		#FIND DATA
		input_paths, input_host, input_filenames = find_data(dbi)
		#pick directory to output to
		output_host = 'folio'
		#MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
		move_feed_files(dbi, input_host, input_paths, output_host, output_dir)
		#ADD FILES TO PAPERDISTILLER ON LIST OF DATA IN NEW LOCATION
		out_dir = os.path.join(output_dir, 'zen.*.uv')
		obs_paths = glob.glob(out_dir)
		distill_files.add_files_to_distill(obs_paths)
	else:
		table = 'Feed'
		move_files.email_space(table)
		time.sleep(21600)

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	feed_bridge(dbi)
