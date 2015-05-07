#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdata

import MySQLdb
import sys
import getpass
import time
import csv
import subprocess
import aipy as A
import hashlib
import glob
import socket
import os
import paramiko
import paperdata_dbi

### Script to add files to paperdata database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def dupe_check(input_host, input_paths):
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).all()
	filenames = tuple(FILE.host, FILE.path, FILE.filename for FILE in FILEs)
	s.close()

	for some_file in filenames:
		if some_file[0] == input_host:
			if os.path.join(*some_path[1:]) not in input_paths:
				return False 

	return True

if __name__ == '__main__':
	input_host = raw_input('Source directory host: ')
	input_paths = glob.glob(raw_input('Source directory path: '))
	input_paths.sort()
	output_host = raw_input('Destination directory host: ')
	output_path = raw_input('Destination directory path: ')
	dupes = dupe_check(input_host, input_paths)
	if not dupes:
		#if any copies, don't load anything
		print 'Duplicate found'
		sys.exit()
