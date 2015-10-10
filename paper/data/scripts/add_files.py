from __future__ import print_function
import os
import sys
import glob
import time
import socket
import uuid
import paper as ppdata
from paper.data import dbi as pdbi, uv_data, file_data
from sqlalchemy import or_

def calc_obs_data(dbi, host, path):
	'''
	generates all relevant data from uv* file

	Parameters
	----------
	host | str: host of system
	path | str: path of uv* file

	Returns
	-------
	tuple:
		dict: observation values
		dict: file values
		dict: log values
	'''
	base_path, filename, filetype = file_data.file_names(path)
	source = ':'.join((host, path))

	if filetype in ('uv', 'uvcRRE'):
		time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_uv_data(host, path)
	elif filetype in ('npz',):
		time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_npz_data(dbi, filename)

	era, julian_day, lst = uv_data.date_info(julian_date)

	filesize = file_data.calc_size(host, path)
	md5 = file_data.calc_md5sum(host, path)

	init_host = host

	timestamp = int(time.time())

	obs_data = {'obsnum': obsnum,
				'julian_date': julian_date,
				'polarization': polarization,
				'julian_day': julian_day,
				'lst': lst,
				'era': era,
				'era_type': None,
				'length': length,
				'time_start': time_start,
				'time_end': time_end,
				'delta_time': delta_time,
				'prev_obs': None, 
				'next_obs': None,
				'is_edge': None,
				'timestamp': timestamp}

	file_data = {'host': host,
				'base_path': base_path,
				'filename': filename,
				'filetype': filetype,
				'source': source,
				'obsnum': obsnum,
				'filesize': filesize,
				'md5sum': md5,
				'tape_index': None,
				'init_host': init_host,
				'is_tapeable': False,
				'is_deletable': False,
				'timestamp': timestamp}

	log_data = {'action': 'add by scan',
				'table': None,
				'identifier': source,
				'log_id': str(uuid.uuid4()),
				'timestamp': timestamp}

	return obs_data, file_data, log_data

def dupe_check(dbi, source_host, source_paths):
	'''
	checks for duplicate paths and removes to not waste time if possible

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: host of uv* files
	source_paths | list[str]: paths of uv* files

	Returns
	-------
	list[str]: paths that are not already in database
	'''
	with dbi.session_scope() as s:
		#all files on same host
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'host') == source_host).all()
		paths = tuple(os.path.join(getattr(FILE, 'base_path'), getattr(FILE, 'filename')) for FILE in FILEs)

	#for each input file, check if in sources
	unique_paths = tuple(source_path for source_path in source_paths if source_path not in paths)
		
	return unique_paths

def set_obs(s, dbi, OBS, field):
	'''
	finds edge observation for each observation by finding previous and next

	Parameters
	----------
	s | object: session object
	dbi | object: database interface object
	OBS | object: observation object
	field | str: field to update

	Returns
	-------
	object: edge observation object
	'''
	if field == 'prev_obs':
		edge_num = getattr(OBS, 'obsnum') - 1
		edge_time = getattr(OBS, 'time_start') - getattr(OBS, 'delta_time')
	elif field == 'next_obs':
		edge_num = getattr(OBS, 'obsnum') + 1
		edge_time = getattr(OBS, 'time_start') + getattr(OBS, 'delta_time')

	table = getattr(pdbi, 'Observation')
	EDGE_OBS = s.query(table).filter(getattr(table, 'julian_date') == edge_time).one()
	if EDGE_OBS is not None:
		edge_obs = getattr(EDGE_OBS, 'obsnum')
		dbi.set_entry(s, OBS, field, edge_obs)
	else:
		pol = getattr(OBS, 'polarization')
		EDGE_OBS = s.query(table).filter(getattr(table, 'julian_date') == edge_time).filter(getattr(table, 'polarization') == pol).one()
		if EDGE_OBS is not None:
			edge_obs = EDGE_OBS.obsnum
			dbi.set_entry(s, OBS, field, edge_obs)

	return EDGE_OBS

def update_obsnums(dbi):
	'''
	updates edge attribute of all obsnums

	Parameters
	----------
	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'Observation')
		OBSs = s.query(table).filter(or_(getattr(table, 'prev_obs') == None, getattr(table, 'next_obs') == None)).all()

		for OBS in OBSs:
			PREV_OBS = set_obs(s, dbi, OBS, 'prev_obs')
			NEXT_OBS = set_obs(s, dbi, OBS, 'next_obs')
			#sets edge 
			is_edge = uv_data.is_edge(PREV_OBS, NEXT_OBS)
			dbi.set_entry(s, OBS, 'is_edge', is_edge)

def connect_observations(dbi):
	'''
	connects file with observation object

	Parameters
	----------
	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		file_table = getattr(pdbi, 'File')
		obs_table = getattr(pdbi, 'Observation')
		FILEs = s.query(file_table).filter(getattr(file_table, 'observation') == None).all()

		for FILE in FILEs:		
			#get the observation corresponding to this file
			OBS = s.query(obs_table).get(getattr(FILE, 'obsnum'))
			dbi.set_entry(s, FILE, 'observation', OBS)  #associate the file with an observation

def update_md5(dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(pdbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
		for FILE in FILEs:
			source = getattr(FILE, 'source')
			timestamp = int(time.time())
			dbi.set_entry(s, FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), source))
			dbi.set_entry(s, FILE, 'timestamp', timestamp)
			log_data = {'action': 'update md5sum',
						'table': 'File',
						'identifier': source,
						'log_id': str(uuid.uuid4()),
						'timestamp': timestamp}

			dbi.add_entry(s, 'Log', log_data)

def add_files_to_db(dbi, source_host, source_paths):
	'''
	adds files to the database

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: host of files
	source_paths | list[str]: paths of uv* files
	'''
	with dbi.session_scope() as s:
		for source_path in source_paths:
			base_path = os.path.dirname(source_path)
			filename = os.path.basename(source_path)
			obs_data, file_data, log_data = calc_obs_data(source_host, source_path)
			try:
				dbi.add_entry_dict(s, 'Observation', obs_data)
			except:
				print('Failed to load in obs ', base_path, filename)
			try:
				dbi.add_entry_dict(s, 'File', file_data)
			except:
				print('Failed to load in file ', base_path, filename)
			try:
				dbi.add_entry_dict(s, 'Log', log_data)
			except:
				print('Failed to load in log ', base_path, filename)

def add_files(dbi, source_host, source_paths):
	'''
	generates list of input files, check for duplicates, add information to database

	Parameters
	----------
	dbi | object: database interface object
	source_host | str: host of files
	source_paths | str: string to indicate paths of uv* files
	'''
	named_host = socket.gethostname()
	if named_host == source_host:
		source_paths = glob.glob(source_paths)
	else:
		with ppdata.ssh_scope(source_host) as ssh:
			source_paths = raw_input('Source directory path: ')
			_, path_out, _ = ssh.exec_command('ls -d {source_paths}'.format(source_paths=source_paths))
			source_paths = path_out.read().split('\n')[:-1]

	source_paths = sorted(dupe_check(dbi, source_host, source_paths))
	npz_paths = [npz_path for npz_path in source_paths if npz_path.endswith('.npz')]
	source_paths = [source_path for source_path in source_paths if not source_path.endswith('.npz')]
	add_files_to_db(dbi, source_host, source_paths)
	add_files_to_db(dbi, source_host, npz_paths)
	update_obsnums(dbi)
	connect_observations(dbi)
	#update_md5(dbi)

if __name__ == '__main__':
	if len(sys.argv) == 2:
		source_host = sys.argv[1].split(':')[0]
		if source_host == sys.argv[1]:
			print('Needs host')
			sys.exit()
		source_paths = sys.argv[1].split(':')[1]
	elif len(sys.argv) == 3:
		source_host = sys.argv[1]
		source_paths = sys.argv[2]
	else:
		source_host = raw_input('Source directory host: ')
		source_paths = raw_input('Source directory path: ')

	dbi = pdbi.DataBaseInterface()
	add_files(dbi, source_host, source_paths)
