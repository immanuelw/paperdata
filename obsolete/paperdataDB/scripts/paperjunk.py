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
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

### Script to load paperfeed with files from the paperrename table
### Checks /data4 for space, moves entire days of data, renames them the correct names, then loads into paperfeed

### Author: Immanuel Washington
### Date: 11-23-14

def calculate_folio_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['df', '-B', '1'])
	#/data4 should be filesystem
	#Amount of available bytes should be free_space

	folio_space = 0
	for output in folio.split('\n'):
		filesystem = output.split(' ')[-1]
		if filesystem == '/data4':
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

def move_files(infile_list, outfile, move_data, usrnm, pswd):
	host = 'folio'

	#Directory of the infiles
	infile_dir = infile_list[0].split('z')[0]

	#create file to log movement data       
	dbo = os.path.join('/data4/paper/junk', move_data)
	dbr = open(dbo,'wb')
	dbr.close()

	o_dict = {}
	for file in infile_list:
		zen = file.split('/')[-1]
		out = os.path.join(outfile,zen)

		o_dict.update({file:out})

	#Load data into named database and table
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	#Load into db
	for infile in infile_list:
		outfile = o_dict[infile]

		#Opens file to append to
		dbr = open(dbo, 'ab')
		wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')

		#"moves" file
		try:
			#scp infile, outfile
			inner = 'obs@' + infile
			subprocess.call(['scp', inner, outfile])
			wr.writerow([infile,outfile])
			print infile, outfile
			dbr.close()
		except:
			dbr.close()
			continue
		# execute the SQL query using execute() method, updates new location
		infile_path = infile
		outfile_path = host + ':' + o_dict[infile]
		cursor.execute('''UPDATE paperjunk set folio_path = %s where junk_path = %s ''', (outfile_path, infile_path))

	print 'File(s) moved and updated'

	#Close database and save changes
	cursor.close()
	connection.commit()
	connection.close()

	return o_dict

def check_paperjunk(max, usrnm, pswd):
	#Load data into named database and table
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT junk_path, junk_size_bytes from paperjunk where folio_path = 'NULL' and junk_size_bytes > 3650722201 and junk_size_bytes < 4187593113 order by usb_number asc, junk_path asc''')
	results = cursor.fetchall()

	junk_size_total = 0
	junk_list = []
	for item in results:
		#checks if space is available
		if junk_size_total < max - 3832908476:
			junk_size = int(item[1])
			junk_file = item[0]

			junk_size_total += junk_size
			junk_list.append(item[0])

	return junk_list

def email_space(table):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	#Send the mail
	msgs = '\nNot enough space for ' + table + ' on folio'

	server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)

	server.quit()

	return None

def paperjunk(auto):
	#Create output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	move_data = 'moved_data_%s.psv'%(time_date)

	#Credentials
	if auto != 'y':
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')

	else:
		usrnm = 'immwa'
		pswd = 'immwa3978'

	#Files to temporarily store information about renamed files
	outfile = '/data4/paper/junk'

	#Checks all filesystems
	dir = '/*'
	folio_space = calculate_folio_space(dir)
	dir = '/data4/paper/junk/'
	free_space = calculate_free_space(dir)

	#Amount of free space needed -- ~4.1TB
	#required_folio_space = 4507997673881
	#2.1TB
	required_folio_space = 2308974418329
	#~4GB
	required_space = 4294967296

	#Move if there is enough free space
	if folio_space >= required_folio_space:
		#move files that haven't been moved
		maximum = free_space - required_space
		infile_list = check_paperjunk(maximum, usrnm, pswd)
		#COPY FILES FROM 1 USB INTO FOLIO
		outfile_dict = move_files(infile_list, outfile, move_data, usrnm, pswd)
	else:
		table = 'paperjunk'
		email_space(table)
		if auto == 'y':
			time.sleep(21600)

	return None

if __name__ == '__main__':
	auto = 'n'
	paperjunk(auto)