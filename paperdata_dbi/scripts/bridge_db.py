#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import sys
import os
import csv
import subprocess
import smtplib
import shutil
import socket
import aipy as A
import hashlib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
from ddr_compress.dbi import DataBaseInterface, Observation, File
from sqlalchemy import func
import paperdata_dbi
import add_files
import uv_data
import move_files

### Script to load infromation quickly from paperdistiller database into paperdata
### Queries paperdistiller for relevant information, loads paperdata with complete info

### Author: Immanuel Washington
### Date: 8-20-14

def calculate_free_space(direc):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['du', '-bs', direc])
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

def add_data():
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
	raw_OBSs = []
	for OBS, jday in j_days.items():
		if jday in complete_jdays:
			raw_OBSs.append(OBS)
	
	#check if day complete
	#if so ignore if already in db
	data_dbi = paperdata_dbi.DataBaseInterface()
	sp = data_dbi.Session()
	#If not, add obs to paperdata, attempt to add files later
	#then create list of tuples of path, filename, other info to load into paperdata

	#need to keep list of files to move of each type -- (host, path, filename, filetype)
	movable_paths = []

	for OBS in raw_OBSs:
		FILE = s.query(File).filter(File.obsnum==OBS.obsnum).one()

		obsnum = OBS.obsnum
		julian_date = OBS.julian_date
		if julian_date < 2456400:
			polarization = 'all'
		else:
			polarization = OBS.pol
		julian_day = int(str(julian_date)[3:7])
		length = round(OBS.length, 5)
	
		host = FILE.host
		full_path = FILE.filename
		path = os.path.dirname(full_path)
		filename = os.path.basename(full_path)
		filetype = filename.split('.')[-1]

		named_host = socket.gethostname()
		if named_host == host:
			try:
				uv = A.miriad.UV(full_path)
			except:
				continue

			time_start = 0
			time_end = 0
			n_times = 0
			c_time = 0

			try:
				for (uvw, t, (i,j)),d in uv.all():
					if time_start == 0 or t < time_start:
						time_start = t
					if time_end == 0 or t > time_end:
						time_end = t
					if c_time != t:
						c_time = t
						n_times += 1
			except:
				continue

			if n_times > 1:
				delta_time = -(time_start - time_end)/(n_times - 1)
			else:
				delta_time = -(time_start - time_end)/(n_times)
		else:
			ssh = paperdata_dbi.login_ssh(host)
			uv_data_script = './uv_data.py'
			sftp = ssh.open_sftp()
			sftp.put(uv_data_script, './')
			sftp.close()
			stdin, uv_data, stderr = ssh.exec_command('python {0} {1} {2}'.format(uv_data_script, host, full_path))
			time_start, time_end, delta_time = [float(info) for info in uv_data.read().split(',')]
			ssh.close()
		
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

		PREV_OBS = sp.query(data_dbi.Observation).filter(data_dbi.Observation.obsnum==obsnum-1).one()
		if PREV_OBS is not None:
			prev_obs = PREV_OBS.obsnum
		else:
			prev_obs = None
		NEXT_OBS = sp.query(data_dbi.Observation).filter(data_dbi.Observation.obsnum==obsnum+1).one()
		if NEXT_OBS is not None:
			next_obs = NEXT_OBS.obsnum
		else:
			next_obs = None
		edge = (None in (prev_obs, next_obs))

		filesize = add_files.calc_size(host, path, filename)
		md5 = add_files.calc_md5sum(host, path, filename)
		tape_index = None

		write_to_tape = True
		delete_file = False

		obs_data = (obsnum, julian_date, polarization, julian_day, era, era_type,
					length, time_start, time_end, delta_time, prev_obs, next_obs, edge)
		data_dbi.add_observation(*obs_data)
		raw_data = (host, path, filename, filetype, obsnum, filesize, md5, tape_index, write_to_tape, delete_file) #cal_path?? XXXX
		data_dbi.add_file(*raw_data)
		movable_paths.append((host, path, filename, filetype))


		compr_filename = filename + 'cRRE'
		compr_filetype = 'uvcRRE'
		compr_filesize = add_files.calc_size(host, path, compr_filename)
		compr_md5 = add_files.calc_md5sum(host, path, compr_filename)
		compr_write_to_tape = False
		if os.path.isdir(compr_filename):
			compr_data = (host, path, compr_filename, compr_filetype, obsnum,
							compr_filesize, compr_md5, tape_index, compr_write_to_tape, delete_file)
			data_dbi.add_file(*compr_data)
			movable_paths.append((host, path, compr_filename, compr_filetype))

		npz_filename = filename + 'cRE.npz'
		npz_filetype = 'npz'
		npz_filesize = add_files.calc_size(host, path, npz_filename)
		npz_md5 = add_files.calc_md5sum(host, path, npz_filename)
		npz_write_to_tape = False
		if os.path.isfile(npz_filename):
			npz_data = (host, path, npz_filename, npz_filetype, obsnum, npz_filesize, npz_md5, tape_index, npz_write_to_tape, delete_file)
			data_dbi.add_file(*npz_data)
			movable_paths.append((host, npz_path, npz_filename, npz_filetype))

	s.close()
	sp.close()

	return movable_paths

def bridge_move(input_host, movable_paths, raw_host, raw_dir, compr_host, compr_dir, npz_host, npz_dir):
	raw_paths = [os.path.join(path[1], path[2]) for path in movable_paths if path[3] == 'uv']
	compr_paths = [os.path.join(path[1], path[2]) for path in movable_paths if path[3] == 'uvcRRE']
	npz_paths = [os.path.join(path[1], path[2]) for path in movable_paths if path[3] == 'npz']

	move_files.move_files(input_host, raw_paths, output_host, raw_dir)
	move_files.move_files(input_host, compr_paths, output_host, compr_dir)
	move_files.move_files(input_host, npz_paths, output_host, npz_dir)

	return None

def paperbridge():
	#Calculate amount of space needed to move a day ~1.1TB
	required_space = 1112661213184

	#Amount of space free in directory
	direc = '/data4/paper/raw_to_tape/'
	free_space = calculate_free_space(direc)

	if free_space <= required_space:
		input_host = raw_input('Source directory host: ')
		#Add observations and paths from paperdistiller
		movable_paths = add_data()
		raw_host = raw_input('Raw destination directory host: ')
		raw_dir = raw_input('Raw destination directory: ')
		compr_host = raw_input('Compressed destination directory host: ')
		compr_dir = raw_input('Compressed destination directory: ')
		npz_host = raw_input('Npz destination directory host: ')
		npz_dir = raw_input('Npz destination directory: ')

		bridge_move(input_host, movable_paths, raw_host, raw_dir, compr_host, compr_dir, npz_host, npz_dir)

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