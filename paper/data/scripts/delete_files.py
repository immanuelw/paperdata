#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paper

from __future__ import print_function
import sys
import time
import subprocess
import glob
import socket
import os
import shutil
import paper as ppdata
from paper.data import dbi as pdbi

### Script to move files and update paper database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def delete_check(dbi, source_host):
	'''
	checks for which files can be deleted

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: host of system

	Returns
	-------
	list[str]: uv* file paths of files to be deleted
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'delete_file') == True).filter(getattr(table, 'tape_index') != None)\
								.filter(getattr(table, 'host') == source_host).all()
	#all files on same host
	paths = tuple(os.path.join(getattr(FILE, 'base_path'), getattr(FILE, 'filename')) for FILE in FILEs)

	return paths

def set_delete_table(s, dbi, source_host, source, dest_host, dest_path):
	'''
	updates table for deleted file

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	source_host | str: user host
	source | str: source file
	dest_host | str: output host
	dest_path | str: output directory
	'''
	full_path = ':'.join((source_host, source))
	timestamp = int(time.time())
	FILE = dbi.get_entry(s, 'File', full_path)
	dbi.set_entry(s, FILE, 'host', dest_host)
	dbi.set_entry(s, FILE, 'base_path', dest_path)
	dbi.set_entry(s, FILE, 'is_deletable', False)
	dbi.set_entry(s, FILE, 'timestamp', timestamp)
	identifier = getattr(FILE, 'full_path')
	log_data = {'action': 'delete',
				'table': 'file',
				'identifier': identifier,
				'timestamp': timestamp}
	dbi.add_entry_dict(s, 'Log', log_data)

def delete_files(dbi, source_host, source_paths, dest_host, dest_path):
	'''
	delete files

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: user host
	source_paths | list[str]: file paths
	dest_host | str: output host
	dest_path | str: output directory
	'''
	named_host = socket.gethostname()
	destination = ':'.join((dest_host, dest_path))
	with dbi.session_scope() as s:
		if named_host == source_host:
			for source in source_paths:
				ppdata.rsync_copy(source, destination)
				set_delete_table(s, dbi, source_host, source, dest_host, dest_path)
				shutil.rmtree(source)
		else:
			with ppdata.ssh_scope(source_host) as ssh:
				for source in source_paths:
					rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
					rsync_del_command = '''rm -r {source}'''.format(source=source)
					ssh.exec_command(rsync_copy_command)
					set_delete_table(s, dbi, source_host, source, dest_host, dest_path)
					ssh.exec_command(rsync_del_command)
	print('Completed transfer')

if __name__ == '__main__':
	source_host = raw_input('Source directory host: ')
	dest_host = raw_input('Destination directory host: ')
	dest_path = raw_input('Destination directory: ')
	source_paths = delete_check(source_host)
	dbi = pdbi.DataBaseInterface()
	delete_files(dbi, source_host, source_paths, dest_host, dest_path)
