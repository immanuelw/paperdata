#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import sys
import os
import time
import subprocess
import smtplib
import shutil
import socket
import aipy as A
import hashlib
import psutil
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
from ddr_compress.dbi import DataBaseInterface, Observation, File
from sqlalchemy import func
import paperdata_dbi as pdbi
import add_files
import uv_data
import move_files

### Script to load infromation quickly from paperdistiller database into paperdata
### Queries paperdistiller for relevant information, loads paperdata with complete info

### Author: Immanuel Washington
### Date: 8-20-14

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

	a_days = {OBS: int(str(OBS.julian_date)[3:7]) for OBS in OBSs_all}
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
	data_dbi = pdbi.DataBaseInterface()
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
		length = round(OBS.length, 5)
	
		host = FILE.host
		full_path = FILE.filename
		path, filename, filetype = add_files.file_names(full_path)

		named_host = socket.gethostname()
		if named_host == host:
			try:
				uv = A.miriad.UV(full_path)
			except:
				continue

			time_start, time_end, delta_time, n_times = add_files.calc_times(uv)

		else:
			ssh = pdbi.login_ssh(host)
			uv_data_script = './uv_data.py'
			sftp = ssh.open_sftp()
			sftp.put(uv_data_script, './')
			sftp.close()
			stdin, uv_data, stderr = ssh.exec_command('python {uv_data_script} {host} {full_path}'.format(uv_data_script=moved_script, host=host, full_path=full_path))
			time_start, time_end, delta_time = [float(info) for info in uv_data.read().split(',')]
			ssh.close()
		
		era, julian_day = add_files.julian_era(julian_date)

		#indicates type of file in era
		era_type = None

		prev_obs, next_obs, edge = add_files.obs_edge(obsnum, sess=sp)

		filesize = add_files.calc_size(host, path, filename)
		md5 = add_files.calc_md5sum(host, path, filename)
		tape_index = None

		source_host = host
		write_to_tape = True
		delete_file = False

		timestamp = int(time.time())

		obs_data = {'obsnum':obsnum,
					'julian_date':julian_date,
					'polarization':polarization,
					'julian_day':julian_day,
					'era':era,
					'era_type':era_type,
					'length':length,
					'time_start':time_start,
					'time_end':time_end,
					'delta_time':delta_time,
					'prev_obs':prev_obs, 
					'next_obs':next_obs,
					'edge':edge,
					'timestamp':timestamp}
		raw_data = {'host':host,
					'path':path,
					'filename':filename,
					'filetype':filetype,
					'full_path':full_path,
					'obsnum':obsnum,
					'filesize':filesize,
					'md5sum':md5,
					'tape_index':tape_index,
					'source_host':source_host,
					'write_to_tape':write_to_tape,
					'delete_file':delete_file,
					'timestamp':timestamp}
		action = 'add by bridge'
		table = None
		log_data = {'action':action,
					'table':table,
					'obsnum':obsnum,
					'full_path':full_path,
					'feed_path':None,
					'timestamp':timestamp}
		pdbi.add_observation(obs_data)
		pdbi.add_file(raw_data)
		pdbi.add_log(log_data)
		movable_paths.append((host, path, filename, filetype))


		compr_filename = filename + 'cRRE'
		compr_filetype = 'uvcRRE'
		compr_filesize = add_files.calc_size(host, path, compr_filename)
		compr_md5 = add_files.calc_md5sum(host, path, compr_filename)
		compr_write_to_tape = False
		if os.path.isdir(compr_filename):
			compr_data = {'host':host,
							'path':path,
							'filename':compr_filename,
							'filetype':compr_filetype,
							'full_path':full_path,
							'obsnum':obsnum,
							'filesize':compr_filesize,
							'md5sum':compr_md5,
							'tape_index':tape_index,
							'write_to_tape':compr_write_to_tape,
							'delete_file':delete_file,
							'timestamp':timestamp}
			pdbi.add_file(compr_data)
			movable_paths.append((host, path, compr_filename, compr_filetype))

		npz_filename = filename + 'cRE.npz'
		npz_filetype = 'npz'
		npz_filesize = add_files.calc_size(host, path, npz_filename)
		npz_md5 = add_files.calc_md5sum(host, path, npz_filename)
		npz_write_to_tape = False
		if os.path.isfile(npz_filename):
			npz_data = {'host':host,
						'path':path,
						'filename':npz_filename,
						'filetype':npz_filetype,
						'obsnum':obsnum,
						'filesize':npz_filesize,
						'md5sum':npz_md5,
						'tape_index':tape_index,
						'write_to_tape':npz_write_to_tape,
						'delete_file':delete_file,
						'timestamp':timestamp}
			pdbi.add_file(npz_data)
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
	free_space = psutil.disk_usage(direc).free

	if free_space >= required_space:
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
