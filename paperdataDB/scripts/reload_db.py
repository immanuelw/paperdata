#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi
import add_files
import os

### Script to reload paperdata database
### Crawls all hosts for files

### Author: Immanuel Washington
### Date: 5-06-15

def find_paths(input_host):
	ssh = paperdata_dbi.login_ssh(input_host)
	input_paths = []
	for root, dirs, files in os.walk('/'):
		for direc in dirs:
			if direc.endswith('uv'):
				 input_paths.append(os.path.join(root, direc))
			elif direc.endswith('uvcRRE'):
				 input_paths.append(os.path.join(root, direc))
		for file_path in files:
			if file_path.endswith('npz'):
				 npz_paths.append(os.path.join(root, file_path))
	ssh.close()			

	return (input_paths, npz_paths)

if __name__ == '__main__':
	if len(sys.argv) == 2:
		input_host = sys.argv[1].split(':')[0]
		if input_host == sys.argv[1]:
			print 'Needs host'
			sys.exit()
	else:
		input_host = raw_input('Source directory host: ')

	input_paths, npz_paths = find_paths(input_host)
	input_paths = add_files.dupe_check(input_host, input_paths)
	input_paths.sort()
	npz_paths = add_files.dupe_check(input_host, npz_paths)
	npz_paths.sort()
	add_files.add_files(input_host, input_paths)
	add_files.add_files(input_host, npz_paths)
