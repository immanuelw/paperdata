#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import time
import subprocess
import smtplib
import shutil
import socket
import aipy as A
import hashlib
import base64
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
from ddr_compress.dbi import DataBaseInterface, Observation, File
from sqlalchemy import func
import paperdata_dbi
import add_files
import uv_data

### Script to load infromation quickly from paperdistiller database into paperdata
### Queries paperdistiller for relevant information, loads paperdata with complete info

### Author: Immanuel Washington
### Date: 8-20-14

def calculate_free_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['du', '-bs', dir])
	#Amount of available bytes should be free_space

	#Do not surpass this amount ~1.2TiB
	max_space = 1319413953331

	total_space = 0
	for output in folio.split('\n'):
		subdir = output.split('\t')[-1]
		if subdir == dir:
			total_space = int(output.split('\t')[0])
	free_space = max_space - total_space

	return free_space

def email_space(table):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	#Send the mail
	header = 'From: PAPERBridge <paperfeed.paperdata@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
	msgs = header + '\nNot enough space for ' + table + ' on folio'

	server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

	server.quit()

	return None

def gen_data():
	dbi = DataBaseInterface()
	s = dbi.Session()
	#do stuff
	OBSs_all = s.query(Observation).all()
	OBSs_complete = s.query(Observation).filter(Observation.status=='COMPLETE').all()

	j_days = {OBS: int(str(OBS.julian_date)[3:7]) for OBS in OBSs_complete}
	count_days = {jday:0 for jday in j_days.values()}
	for jday in j_days.values():
		count_days[jday] += 1

	a_days = {(OBS: int(str(OBS.julian_date)[3:7]) for OBS in OBSs_all}
	all_days = {jday:0 for jday in a_days.values()}
	for jday in a_days.values():
		all_days[jday] += 1

	#tuple list of all complete days
	complete_jdays = tuple(day for day in count_days.keys() if count_day[day]==all_day[day])
	complete_OBSs = []
	for OBS, jday in j_days.items():
		if jday in complete_jdays:
			complete_OBSs.append(OBS)
	
	#check if day complete
	#if so ignore if already in db
	data_dbi = paperdata_dbi.DataBaseInterface()
	sp = data_dbi.Session()
	#If not, add obs to paperdata, attempt to add files later
	#then create list of tuples of path, filename, other info to load into paperdata

	for OBS in complete_OBSs:
		FILE = s.query(File).filter(File.obsnum==OBS.obsnum).one()

		obsnum = OBS.obsnum
		julian_date = OBS.julian_date
		polarization = OBS.pol
		julian_day = int(str(julian_date)[3:7])
	
		#time_start, time_end, delta_time, length = uv_data()
		
		#indicates julian day and set of data
		if julian_date < 2456100:
			era = 32
		elif julian_date < 2456400:
			era = 64
		else:
			era = 128

		#indicates type of file in era
		era_type = 'NULL'

		#location of calibrate files
		#if era == 32:
		#	cal_path = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
		#elif era == 64:
		#	cal_path = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'
		#elif era == 128:
		#	cal_path = 'NULL'

		PREV_OBS = sp.query(dbi.Observation).filter(dbi.Observation.obsnum==obsnum-1).one()
		if PREV_OBS is not None:
			prev_obs = PREV_OBS.obsnum
		else:
			prev_obs = None
		NEXT_OBS = sp.query(dbi.Observation).filter(dbi.Observation.obsnum==obsnum+1).one()
		if NEXT_OBS is not None:
			next_obs = NEXT_OBS.obsnum
		else:
			next_obs = None
		edge = (None in (prev_obs, next_obs))

		host = FILE.host
		full_path = FILE.filename
		path = os.path.dirname(full_path)
		filename = os.path.basename(full_path)
		filesize = add_files.calc_size(host, path, filename)
		md5 = add_files.calc_md5sum(host, path, filename)
		tape_index = None

		write_to_tape = False
		delete_file = False

		obs_data = (obsnum, julian_date, polarization, julian_day, era, era_type,
					length, time_start, time_end, delta_time, prev_obs, next_obs, edge)
		data_dbi.add_observation(*obs_data)
		file_data = (host, path, filename, filetype, obsnum, filesize, md5, tape_index, write_to_tape, delete_file) #cal_path?? XXXX
		data_dbi.add_file(*file_data)
	s.close()
	sp.close()

	return file_paths

def move_files(infile_list, outfile, move_data, usrnm, pswd):
	host = 'folio'

	#create file to log movement data	   
	dbo = os.path.join(outfile, move_data)
	dbr = open(dbo,'wb')
	dbr.close()

	o_dict = {}
	for file in infile_list:
		zen = file.split('/')[-1]
		psa = file.split('.')[-4]

		subdir = os.path.join(psa,zen)
		outdir = os.path.join(outfile,psa)

		if not os.path.isdir(outdir):
			os.mkdir(outdir)

		out = os.path.join(outfile,subdir)

		o_dict.update({file:out})

	#Load data into named database and table
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	#Load into db
	for infile in infile_list:
		if infile.split('.')[-1] != 'uv':
			print 'Invalid file type'
			sys.exit()

		outfile = o_dict[infile]

		#Opens file to append to
		dbr = open(dbo, 'ab')
		wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')

		#"moves" file
		try:
			inner = infile.split(':')[1]
			shutil.move(inner, outfile)
			wr.writerow([infile,outfile])
			print infile, outfile
			dbr.close()
		except:
			dbr.close()
			continue
		# execute the SQL query using execute() method, updates new location
		infile_path = infile
		outfile_path = host + ':' + o_dict[infile]
		if infile.split('.')[-1] == 'uv':
			cursor.execute('''UPDATE paperdata set raw_path = %s, write_to_tape = 1 where raw_path = %s ''', (outfile_path, infile_path))

	print 'File(s) moved and updated'

	#Close database and save changes
	cursor.close()
	connection.commit()
	connection.close()

	return None

def move_compressed_files(infile_list, outfile, move_data, usrnm, pswd):
	host = 'folio'

	#create file to log movement data	   
	dbo = os.path.join(outfile, move_data)
	dbr = open(dbo,'wb')
	dbr.close()

	o_dict = {}
	for file in infile_list:
		zen = file.split('/')[-1]
		psa = file.split('.')[-4]

		subdir = os.path.join(psa,zen)
		outdir = os.path.join(outfile,psa)

		if not os.path.isdir(outdir):
			os.mkdir(outdir)

		out = os.path.join(outfile,subdir)

		o_dict.update({file:out})

	#Load data into named database and table
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	#Load into db
	for infile in infile_list:
		if infile.split('.')[-1] != 'uvcRRE':
			print 'Invalid file type'
			sys.exit()

		infile_npz = infile.split('uvcRRE')[0] + 'uvcRE.npz'
		infile_final_product = infile.split('uvcRRE')[0] + 'uvcRREzCPSBx'

		infile_npz_path = ''
		infile_final_path = ''

		outfile = o_dict[infile]

		#Opens file to append to
		dbr = open(dbo, 'ab')
		wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')

		npz_path = outfile.split('uvcRRE')[0] + 'uvcRE.npz'
		final_product_path = outfile.split('uvcRRE')[0] + 'uvcRREzCPSBx'

		#"moves" file
		try:
			inner = infile.split(':')[1]
			shutil.move(inner, outfile)
			wr.writerow([infile,outfile])
			print infile, outfile
			try:
				if os.path.isfile(infile_npz):
					inner_npz = infile_npz.split(':')[1]
					shutil.move(inner_npz, npz_path)
					wr.writerow([inner_npz,npz_path])
					print inner_npz, npz_path
				else:
					infile_npz_path = 'NULL'
					outfile_npz_path = 'NULL'
				if os.path.isdir(infile_final_product):
					inner_final = infile_final_product.split(':')[1]
					shutil.move(inner_final, final_product_path)
					wr.writerow([inner_final,final_product_path])
					print inner_final, final_product_path
				else:
					infile_final_path = 'NULL'
					outfile_final_path = 'NULL'
				dbr.close()
			except:
				dbr.close()
				continue
		except:
			dbr.close()
			continue
		# execute the SQL query using execute() method, updates new location
		infile_path = infile
		outfile_path = host + ':' + o_dict[infile]
		if infile_npz_path != 'NULL':
			infile_npz_path = infile_npz
			outfile_npz_path = host + ':' + npz_path
		if infile_final_path != 'NULL':
			infile_final_path = infile_final_product
			outfile_final_path = host + ':' + final_product_path
		if infile.split('.')[-1] == 'uvcRRE':
			cursor.execute('''UPDATE paperdata set path = %s, npz_path = %s, final_product_path = %s where path = %s and npz_path = %s and final_product_path = %s ''', (outfile_path, outfile_npz_path, outfile_final_path, infile_path, infile_npz_path, infile_final_path))

	print 'File(s) moved and updated'

	#Close database and save changes
	cursor.close()
	connection.commit()
	connection.close()

	return None

def paperbridge(auto):
	#User input information
	if auto != 'y':
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')

		auto_load = raw_input('Automatically load into paperdata? (y/n): ')

	else:
		usrnm = 'jaguirre'
		pswd = base64.b64decode('amFndWlycmU2OTE5')
		auto_load = 'y'

	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	dbnum = '/data4/paper/paperdistiller_output/paperdistiller_output_%s.psv'%(time_date)
	dbe = '/data4/paper/paperdistiller_output/paperdistiller_error_%s.psv'%(time_date)

	#Calculate amount of space needed to move a day ~1.1TB
	required_space = 1112661213184

	#Amount of space free in directory
	dir = '/data4/paper/raw_to_tape/'
	free_space = calculate_free_space(dir)

	if free_space <= required_space:
		#Pull information from paperdistiler
		results, obsnums, filenames, filenames_c = gen_data_list(usrnm,pswd)

		#Generate data from info pulled
		gen_data_from_paperdistiller(results, obsnums, dbnum, dbe)

		#check if auto-loading
		if auto_load == 'y':
			#Load information into paperdata
			table = 'paperdata'
			load_paperdata.load_db_from_file(dbnum, table, usrnm, pswd)
			#Update paperdata and move data
			move_data = 'moved_data_%s.psv'%(time_date)	
			#outfile = '/data4/paper/raw_to_tape'
			#move_files(filenames, outfile, move_data, usrnm, pswd)
			#outfile_c = '/data4/paper/2013EoR'
			#move_compressed_files(filenames_c, outfile_c, move_data, usrnm, pswd)
		else:
			print '''Information logged into '%s' ''' %(dbnum)

	else:
		table = 'paperdistiller'
		email_space(table)
		if auto == 'y':
			time.sleep(14400)

	return None

if __name__ == '__main__':
	auto = 'n'
	paperbridge(auto)
	add_files.update_obsnums()
