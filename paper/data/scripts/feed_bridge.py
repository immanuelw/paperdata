from __future__ import print_function
import os
import time
import shutil
import socket
import paper as ppdata
from paper.data import dbi as pdbi
import paper.memory as memory
import distill_files
import move_files
from sqlalchemy import func
from sqlalchemy.sql import label

def set_feed(s, dbi, source_path, dest_host, dest_path, is_moved=True):
	'''
	updates table for feed file

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	source_path | str: source file path
	dest_host | str: output host
	dest_path | str: output directory
	is_moved | bool: checks whether to move to paperdistiller --defaults to False
	'''
	FEED = dbi.get_entry(s, 'Feed', source_path)
	dbi.set_entry(s, FEED, 'host', dest_host)
	dbi.set_entry(s, FEED, 'base_path', dest_path)
	dbi.set_entry(s, FEED, 'is_moved', is_moved)

def move_feed_files(dbi, source_host, source_paths, dest_host, dest_path):
	'''
	moves files and adds to feed directory and table

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: file host
	source_paths | list[str]: file paths
	dest_host | str: output host
	dest_path | str: output directory
	'''
	#different from move_files, adds to feed
	named_host = socket.gethostname()
	destination = ':'.join((dest_host, dest_path))
	with dbi.session_scope() as s:
		if named_host == source_host:
			for source_path in source_paths:
				ppdata.rsync_copy(source_path, destination)
				set_feed(s, dbi, source_path, dest_host, dest_path)
				shutil.rmtree(source_path)
		else:
			with ppdata.ssh_scope(source_host) as ssh:
				for source_path in source_paths:
					rsync_copy_command = '''rsync -ac {source_path} {destination}'''.format(source_path=source_path, destination=destination)
					rsync_del_command = '''rm -r {source_path}'''.format(source_path=source_path)
					ssh.exec_command(rsync_copy_command)
					set_feed(s, dbi, source_path, dest_host, dest_path)
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

		to_move = (getattr(FEED, 'source') for FEED in all_FEEDs if getattr(FEED, 'julian_day') in good_days)

		for path in to_move:
			FEED = dbi.get_entry(s, 'Feed', path)
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
		str: file host
		list[str]: file paths to move
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Feed')
		FEEDs = s.query(table).filter(getattr(table, 'is_moved') == False).filter(getattr(table, 'is_movable') == True).all()

	#only move one day at a time
	feed_day = getattr(FEEDs[0], 'julian_day')
	feed_host = getattr(FEEDs[0], 'host')
	feed_paths = tuple(os.path.join(getattr(FEED, 'path'), getattr(FEED, 'filename'))
						for FEED in FEEDs if getattr(FEED, 'julian_day') == feed_day)

	return feed_paths, feed_host

def feed_bridge(dbi):
	'''
	bridges feed and paperdistiller
	moves files and pulls relevant data to add to paperdistiller from feed

	Parameters
	----------
	dbi | object: database interface object
	'''
	#Minimum amount of memory to move a day ~3.1TiB
	required_memory = 1112373311360
	dest_path = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED

	#Move if there is enough free memory
	if memory.enough_memory(required_memory, dest_path):
		#check how many days are in each
		count_days(dbi)
		#FIND DATA
		source_host, source_paths = find_data(dbi)
		#pick directory to output to
		dest_host = 'folio'
		#MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
		move_feed_files(dbi, source_host, source_paths, dest_host, dest_path)
		#ADD FILES TO PAPERDISTILLER ON LIST OF DATA IN NEW LOCATION
		out_dir = os.path.join(dest_path, 'zen.*.uv')
		obs_paths = glob.glob(out_dir)
		distill_files.add_files_to_distill(obs_paths)
	else:
		table = 'Feed'
		memory.email_memory(table)
		time.sleep(21600)

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	feed_bridge(dbi)
