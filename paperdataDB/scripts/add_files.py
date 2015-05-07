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
	s.close()
	#all files on same host
	filenames = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs if FILE.host == input_host)

	#for each input file, check if in filenames
	duplicates = tuple(input_path for input_path in input_paths if input_path in filenames)
		
	if len(duplicates) > 0:
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
