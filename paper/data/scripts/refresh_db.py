'''
paper.data.scripts.refresh_db

refreshes database by fixing possible issues due to moved files or wrong values for observations

author | Immanuel Washington

Functions
---------
path_exists | checks for path existence and returns boolean
set_obs | sets edge information for observations
update_obsnums | updates observation previous and next obsnums
connect_observations | connects observations with files in database
update_md5 | updates md5 checksums if needed
update_sources | checks for file sources and removes if nonexistent
'''
from __future__ import print_function
import os
import socket
import time
import uuid
import paper as ppdata
from paper.data import dbi as pdbi, uv_data, file_data
from sqlalchemy import or_

def path_exists(sftp, path):
	'''
	checks for path existence

	Parameters
	----------
	sftp | object: SFTP object
	path | str: path of file or directory

	bool: path exists
	'''
	try:
		sftp.stat(path)
		return True
	except IOError:
		return False

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
		edge_num = OBS.obsnum - 1
		edge_time = OBS.time_start - OBS.delta_time
	elif field == 'next_obs':
		edge_num = OBS.obsnum + 1
		edge_time = OBS.time_start + OBS.delta_time

	table = pdbi.Observation
	EDGE_OBS = s.query(table).filter(table.julian_date == edge_time).one()
	if EDGE_OBS is not None:
		edge_obs = EDGE_OBS.obsnum
		dbi.set_entry(s, OBS, field, edge_obs)
	else:
		pol = OBS.polarization
		EDGE_OBS = s.query(table).filter(table.julian_date == edge_time).filter(table.polarization == pol).one()
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
		table = pdbi.Observation
		OBSs = s.query(table).filter(or_(table.prev_obs == None, table.next_obs == None)).all()

		for OBS in OBSs:
			PREV_OBS = set_obs(s, dbi, OBS, 'prev_obs')
			NEXT_OBS = set_obs(s, dbi, OBS, 'next_obs')
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
		file_table = pdbi.File
		obs_table = pdbi.Observation
		FILEs = s.query(file_table).filter(file_table.observation == None).all()

		for FILE in FILEs:		
			OBS = s.query(obs_table).get(FILE.obsnum)
			dbi.set_entry(s, FILE, 'observation', OBS)

def update_md5(dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		table = pdbi.File
		FILEs = s.query(table).filter(table.md5sum == None).all()
		for FILE in FILEs:
			source = FILE.source
			timestamp = int(time.time())
			md5_dict = {'md5sum': file_data.calc_md5sum(FILE.host, source),
						'timestamp': timestamp}
			dbi.set_entry_dict(s, FILE, md5_dict)

			log_data = {'action': 'update md5sum',
						'table': 'File',
						'identifier': source,
						'log_id': str(uuid.uuid4()),
						'timestamp': timestamp}
			dbi.add_entry(s, 'Log', log_data)

def update_paths(dbi):
	'''
	fixes database files and directories that have been moved/deleted

	Parameters
	----------
	dbi | object: database interface object
	'''
	source_host = socket.gethostname()
	table = pdbi.File
	hosts = ('folio', 'node16', 'nas1', 'nas2', 'pot1', 'pot2', 'pot3', 'pot4', 'pot8')
	with dbi.session_scope() as s:
		for host in hosts:
			FILEs = s.query(table).filter(table.host == host).all()
			if source_host == host:
				for FILE in FILEs:
					if not os.path.exists(FILE.source):
						s.delete(FILE)
			else:
				for FILE in FILEs:
					with ppdata.ssh_scope(host) as ssh:
						with ssh.open_sftp() as sftp:
							if not path_exists(sftp, FILE.source):
								s.delete(FILE)

def refresh_db(dbi):
	'''
	refreshes database by checking md5sums, paths, obsnums
	connects observations to files

	Parameters
	----------
	dbi | object: database interface object
	'''
	update_paths(dbi)
	update_md5(dbi)
	update_obsnums(dbi)
	connect_observations(dbi)

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	refresh_db(dbi)
