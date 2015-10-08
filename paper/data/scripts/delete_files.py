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

def delete_check(dbi, input_host):
	'''
	checks for which files can be deleted

	Parameters
	----------
	dbi | object: database interface object
		host | str: host of system

	Returns
	-------
	list: uv* file paths of files to be deleted
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'delete_file') == True).filter(getattr(table, 'tape_index') != None)\
								.filter(getattr(table, 'host') == input_host).all()
	#all files on same host
	full_paths = tuple(os.path.join(getattr(FILE, 'path'), getattr(FILE, 'filename')) for FILE in FILEs)

	return full_paths

def set_delete_table(s, dbi, input_host, source, output_host, output_dir):
	'''
	updates table for deleted file

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	input_host | str: user host
	source | str: source file
	output_host | str: output host
	output_dir | str: output directory
	'''
	full_path = ''.join((input_host, ':', source))
	timestamp = int(time.time())
	FILE = dbi.get_entry(s, 'File', full_path)
	dbi.set_entry(s, FILE, 'host', output_host)
	dbi.set_entry(s, FILE, 'path', output_dir)
	dbi.set_entry(s, FILE, 'is_deletable', False)
	dbi.set_entry(s, FILE, 'timestamp', timestamp)
	identifier = getattr(FILE, 'full_path')
	log_data = {'action': 'delete',
				'table': 'file',
				'identifier': identifier,
				'timestamp': timestamp}
	dbi.add_entry_dict(s, 'Log', log_data)

	return None

def delete_files(dbi, input_host, input_paths, output_host, output_dir):
	'''
	delete files

	Parameters
	----------
	dbi | object: database interface object
	input_host | str: user host
	input_paths | list: file paths
	output_host | str: output host
	output_dir | str: output directory
	'''
	named_host = socket.gethostname()
	destination = ''.join((output_host, ':', output_dir))
	if named_host == input_host:
		with dbi.session_scope() as s:
			for source in input_paths:
				ppdata.rsync_copy(source, destination)
				set_delete_table(s, dbi, input_host, source, output_host, output_dir)
				shutil.rmtree(source)
	else:
		with ppdata.ssh_scope(input_host) as ssh:
			for source in input_paths:
				rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
				rsync_del_command = '''rm -r {source}'''.format(source=source)
				ssh.exec_command(rsync_copy_command)
				set_delete_table(input_host, source, output_host, output_dir)
				ssh.exec_command(rsync_del_command)
	print('Completed transfer')

	return None

if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	output_host = raw_input('Destination directory host: ')
	output_dir = raw_input('Destination directory: ')
	input_paths = delete_check(input_host)
	dbi = pdbi.DataBaseInterface()
	delete_files(dbi, input_host, input_paths, output_host, output_dir)
