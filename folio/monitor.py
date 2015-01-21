#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import glob
import socket
import time
import subprocess

### Script to check the status of folio at any point
### Checks /data4 for space, uses iostat to check for cpu usage and I/O statistics

### Author: Immanuel Washington
### Date: 01-20-15

def calculate_folio_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['df', '-B', '1'])
	#/data4 should be filesystem
	#Amount of available bytes should be free_space

	folio_space = 0
	for output in folio.split('\n'):
		filesystem = output.split(' ')[-1]
		if filesystem in ['/data4']:
			folio_space = int(output.split(' ')[-4])
	return folio_space

def calculate_free_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['du', '-bs', dir])
	#Amount of available bytes should be free_space

	#Do not surpass this amount ~1TiB
	#max_space = 1099511627776
	#1.1TB
	max_space = 1209462790553

	total_space = 0
	for output in folio.split('\n'):
		subdir = output.split('\t')[-1]
		if subdir == dir:
			total_space = int(output.split('\t')[0])
	free_space = max_space - total_space

	return free_space

def iostat():
	#Calculates cpu usage on folio nodes
	folio = subprocess.check_output(['iostat'])

	folio_use = []
	folio_name = folio.split('\n')[0].split(' ')[2].strip('()')
	for output in folio.split('\n'):
		device = output.split(' ')[0]
		if device in ['Device:', 'sda', 'sda1', 'sda2','dm-0', 'dm-1']:
			line = output[:].split(' ')
			new_line = filter(lambda a: a not in [''], line)
			folio_use.append(new_line)

	return [folio_name, folio_use]

def write_file(folio_data, time_date, folio_space, free_space, host_name, usage):
	dbr = open(folio_data, 'ab')
	wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')
	wr.writerow([time_date])
	wr.writerow(['Space on folio', folio_space])
	wr.writerow(['Free space on folio', free_space])
	wr.writerow(['Host', host_name])
	wr.writerow(['CPU statistics:'])
	for row in usage:
		wr.writerow(row)

	dbr.close()

	return None

def monitor(auto):
	#Create output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	folio_data = '/data4/paper/paperoutput/folio_data.psv'
	dbr = open(folio_data, 'wb')
	dbr.close()

	#Checks all filesystems
	dir = '/*'
	folio_space = calculate_folio_space(dir)
	dir = '/data4/paper/junk/'
	free_space = calculate_free_space(dir)

	host_name, usage = iostat()

	write_file(folio_data, time_date, folio_space, free_space, host_name, usage)

	if auto in ['y']:
		time.sleep(60)

	return None
if __name__ == '__main__':
	auto = 'n'
	monitor(auto)
