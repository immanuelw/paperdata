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
import pyganglia as pyg
import sys
import jdcal

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

	device = ['Device:', 'sda', 'sda1', 'sda2','dm-0', 'dm-1']
	#Convert all numbers to floats, keep words as strings
	folio_use = [[float(i) if i not in device else i for i in j] for j in folio_use[1:]]

	return [folio_name, folio_use]

def ram_free():
	#Calculates ram usage on folio
	folio = subprocess.check_output(['free', '-b'])
	ram = []
	for output in folio.split('\n'):
		line = output[:].split(' ')
		new_line = filter(lambda a: a not in [''], line)
		ram.append(new_line)	

	reram = []
	for key, row in enumerate(ram[1:-1]):
		if key in [0,2]:
				reram.extend([int(i) for i in row[1:]])
		if key in [1]:
				reram.extend([int(i) for i in row[2:]])

	return [reram]

def cpu_perc():
	#Calculates cpu usage on folio
	folio = subprocess.check_output(['mpstat', '-P', 'ALL', '1', '1'])
	cpu = []
	for output in folio.split('\n'):
		if output in [folio.split('\n')[0],folio.split('\n')[1]]:
			continue
		line = output[:].split(' ')
		new_line = filter(lambda a: a not in [''], line)
		if new_line[0] not in ['Average:']:
			cpu.append(new_line)

	recpu = []
	for row in cpu[2:]:
		dummy_cpu = []
		for key, item in enumerate(row):
			if key in [2,3,5,6,10,11]:
				dummy_cpu.append(float(item))
		recpu.append(dummy_cpu)

	return recpu

"""
def processes():
	#Calculates cpu usage on folio
	folio = subprocess.check_output(['top', '-n1'])
	pro = []
	#for output in folio.split('\n'):
	for output in folio.split('\n'):
		if output in folio.split('\n')[:6]:
			continue
		line = output[:].split(' ')
		new_line = filter(lambda a: a not in [''], line)
		pro.append(new_line[1:-1])

	return pro
"""

#def write_file(usrnm, pswd, folio_data, time_date, folio_space, host_name, usage, ram, cpu, pro):
def write_file(usrnm, pswd, folio_data, time_date, folio_space, host_name, usage, ram, cpu):
	dbr = open(folio_data, 'ab')
	wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')
	wr.writerow([time_date])
	wr.writerow(['Space on folio', folio_space])
	wr.writerow(['Host', host_name])
	wr.writerow(['Average CPU statistics:'])
	for row in usage:
		wr.writerow(row)
	wr.writerow(['RAM:'])
	for row in ram:
		wr.writerow(row)
	wr.writerow(['CPU Usage:'])
	for row in cpu:
		wr.writerow(row)
	#wr.writerow(['Processes:'])
	#for row in pro:
	#	wr.writerow(row)

	dbr.close()

	return None

def add_to_db(usrnm, pswd, host_name, time_date, usage, ram, cpu):

	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	#execute the SQL query using execute() method.
	insert_base = '''INSERT INTO %s VALUES(%s)'''
	for row in usage:
		val = tuple(row)
		insert = insert_base % (pyg.iostat().table, pyg.iostat().values)
		values = (host_name,) + val + (time_date,)
		cursor.execute(insert, values)
	for row in ram:
		val = tuple(row)
		insert = insert_base % (pyg.ram().table, pyg.ram().values)
		values = (host_name,) + val + (time_date,)
		cursor.execute(insert, values)
	for row in cpu:
		val = tuple(row)
		insert = insert_base % (pyg.cpu().table, pyg.cpu().values)
		values = (host_name,) + val + (time_date,)
		cursor.execute(insert, values)

	#Close and save changes to database
	cursor.close()
	connection.commit()
	connection.close()
	
	return None

def data_out(time_date):
	#Create output file
	time_day = time.strftime('%d-%m-%Y')
	host = socket.gethostname()
	folio_data = '/data4/paper/paperoutput/log_%s_%s.psv' %(host, time_day)
	print folio_data
	dbr = open(folio_data, 'wb')
	dbr.close()

	return folio_data

def monitor(auto):
	time_date = int(time.strftime('%Y%m%d%H%M%S'))

	#Create output file

	start_time = time.time()
	interval = 60

	if auto not in ['y']:
		if len(sys.argv) > 1:
			auto = sys.argv[1]

	if auto in ['y']:
		usrnm = 'immwa'
		pswd = 'immwa3978'
	else:
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')

	folio_data = data_out(time_date)

	#run for 3 days
	#for i in range(4320):
	while True:
		time_date = time.strftime('%Y:%m:%d:%H:%M:%S')
		temp_time = time_date.split(':')
		time_date = jdcal.gcal2jd(temp_time[0],temp_time[1],temp_time[2],temp_time[3],temp_time[4],temp_time[5])

		#Checks all filesystems
		dir = '/*'
		folio_space = calculate_folio_space(dir)

		host_name, usage = iostat()
		ram = ram_free()
		cpu = cpu_perc()
		#pro = processes()

		#write_file(usrnm, pswd, folio_data, time_date, folio_space, host_name, usage, ram, cpu, pro)
		write_file(usrnm, pswd, folio_data, time_date, folio_space, host_name, usage, ram, cpu)
		add_to_db(usrnm, pswd, host_name, time_date, usage, ram, cpu)
		time.sleep(start_time + (i + 1) * interval - time.time())

	#if auto in ['y']:
	#	time.sleep(60)

	return None
if __name__ == '__main__':
	auto = 'n'
	monitor(auto)
