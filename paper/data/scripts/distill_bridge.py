#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import sys
import os
import copy
import time
import socket
from collections import Counter
import aipy as A
import ddr_compress.dbi as ddbi
from paper.data import dbi as pdbi, uv_data, file_data
import add_files, move_files

### Script to load infromation quickly from paperdistiller database into paper
### Queries paperdistiller for relevant information, loads paper with complete info

### Author: Immanuel Washington
### Date: 8-20-14
def add_data(dbi, data_dbi):
	'''
	transfer data from paperdistiller database to create data for paperdata tables

	Args:
		dbi (object): distiller database interface object
		data_dbi (object): data database interface object

	Returns:
		dict: movable paths for each filetype
	'''
	s = dbi.Session()
	#do stuff
	table = getattr(ddbi, 'Observation')
	OBSs_all = s.query(table).all()
	OBSs_complete = s.query(table).filter(getattr(table, 'status') == 'COMPLETE').all()

	julian_obs = {OBS: int(getattr(OBS, 'julian_date')) for OBS in OBSs_complete}
	julian_days = tuple(jday for jday in julian_obs.values())
	#dict of julian day as key, amount as value
	count_jdays = Counter(julian_days)

	all_days = tuple(int(getattr(OBS, 'julian_date')) for OBS in OBSs_all)
	count_all_days = Counter(all_days)

	#tuple list of all complete days
	complete_jdays = tuple(day for day, amount in count_jdays.items() if amount == count_all_days[day])
	raw_OBSs = tuple(OBS for OBS, jday in julian_obs.items() if jday in complete_jdays)

	with data_dbi.session_scope():
		#need to keep dict of list of files to move of each type -- (host, path, filename, filetype)
		movable_paths = {'uv':[], 'uvcRRE':[], 'npz':[]}

		named_host = socket.gethostname()
		for OBS in raw_OBSs:
			table = getattr(ddbi, 'File')
			FILE = s.query(table).filter(getattr(table, 'obsnum') == getattr(OBS, 'obsnum')).one()

			host = getattr(FILE, 'host')
			full_path = getattr(FILE, 'filename')
			path, filename, filetype = file_data.file_names(full_path)

			obsnum = getattr(OBS, 'obsnum')
			julian_date = getattr(OBS, 'julian_date')
			if julian_date < 2456400:
				polarization = 'all'
			else:
				polarization = getattr(OBS, 'pol')
			length = getattr(OBS, 'length')
		
			if named_host == host:
				try:
					uv = A.miriad.UV(full_path)
				except:
					continue

				time_start, time_end, delta_time, _  = uv_data.calc_times(uv)

			else:
				time_start, time_end, delta_time, _ = add_files.get_uv_data(host, full_path, mode='time')

			lst = uv_data.get_lst(julian_date)		
			era, julian_day = uv_data.julian_era(julian_date)

			#indicates type of file in era
			era_type = None

			prev_obs = None
			next_obs = None
			edge = None

			filesize = file_data.calc_size(host, path, filename)
			md5 = getattr(FILE, 'md5sum')
			if md5 is None:
				md5 = file_data.calc_md5sum(host, path, filename)
			tape_index = None

			source_host = host
			write_to_tape = True
			delete_file = False

			timestamp = int(time.time())

			obs_data = {'obsnum':obsnum,
						'julian_date':julian_date,
						'polarization':polarization,
						'julian_day':julian_day,
						'lst':lst,
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
			identifier = full_path
			log_data = {'action':action,
						'table':table,
						'identifier':identifier,
						'timestamp':timestamp}
			data_dbi.add_entry_dict(sess, 'observation', obs_data)
			data_dbi.add_entry_dict(sess, 'file', raw_data)
			data_dbi.add_entry_dict(sess, 'log', log_data)
			movable_paths[filetype].append(full_path)

			compr_filename = ''.join((filename, 'cRRE'))
			compr_path = os.path.join(path, compr_filename)
			if os.path.isdir(compr_path):
				compr_filetype = 'uvcRRE'
				compr_data = copy.deepcopy(raw_data)
				compr_data['filename'] = compr_filename
				compr_data['filetype'] = compr_filetype
				compr_data['filesize'] = file_data.calc_size(host, path, compr_filename)
				compr_data['md5sum'] = file_data.calc_md5sum(host, path, compr_filename)
				compr_data['write_to_tape'] = False
				data_dbi.add_entry_dict(sess, 'file', compr_data)
				movable_paths[compr_filetype].append(compr_path)

			npz_filename = ''.join((filename, 'cRE.npz'))
			npz_path = os.path.join(path, npz_filename)
			if os.path.isdir(npz_path):
				npz_filetype = 'npz'
				npz_data = copy.deepcopy(raw_data)
				npz_data['filename'] = npz_filename
				npz_data['filetype'] = npz_filetype
				npz_data['filesize'] = file_data.calc_size(host, path, npz_filename)
				npz_data['md5sum'] = file_data.calc_md5sum(host, path, npz_filename)
				npz_data['write_to_tape'] = False
				data_dbi.add_entry_dict(sess, 'file', npz_data)
				movable_paths[npz_filetype].append(npz_path)
	s.close()

	return movable_paths

def paperbridge(dbi, data_dbi, auto=False):
	'''
	bridges paperdistiller and paperdata
	moves files and pulls relevant data to add to paperdata from paperdistiller

	Args:
		dbi (object): distiller database interface object
		data_dbi (object): data database interface object
		auto (bool): track whether to wait -- defaults to False
	'''
	#Calculate amount of space needed to move a day ~1.1TB
	required_space = 1112661213184
	space_path = '/data4/paper/raw_to_tape/'

	if move_files.enough_space(required_space, space_path):
		input_host = raw_input('Source directory host: ')
		#Add observations and paths from paperdistiller
		movable_paths = add_data(dbi, data_dbi)
		filetypes = movable_paths.keys()
		host_dirs = {filetype: {'host': None, 'dir': None} for filetype in filetypes}
		for filetype in filetypes:
			host_dirs[filetype]['host'] = raw_input('{filetype} destination directory host: '.format(filetype=filetype))
			host_dirs[filetype]['dir'] = raw_input('{filetype} destination directory: '.format(filetype=filetype))

		for filetype, paths in movable_paths:
			move_files.move_files(dbi, input_host, paths, host_dirs[filetype]['host'], host_dirs[filetype]['dir'])

	else:
		table = 'paperdistiller'
		move_files.email_space(table)
		if auto:
			time.sleep(14400)

	return None

if __name__ == '__main__':
	dbi = ddbi.DataBaseInterface()
	data_dbi = pdbi.DataBaseInterface()
	paperbridge(dbi, data_dbi)
	add_files.update_obsnums(data_dbi)
	add_files.connect_observations(data_dbi)
