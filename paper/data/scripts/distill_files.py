'''
paper.data.scripts.distill_files

adds files to paperdistiller database

author | Immanuel Washington

Functions
---------
file2jd | pulls julian date from filename
file2pol | pulls polarization from filename
add_files_to_distill | adds files to paperdistiller database after pulling some relevant information
'''
from __future__ import print_function
import os
import sys
import glob
import re
import socket
import numpy as n
from paper.data import uv_data
from paper.distiller import dbi as ddbi

def file2jd(path):
	'''
	pulls julian date from filename using regex

	Parameters
	----------
	path | str: path of file

	Returns
	-------
	float(5): julian date
	'''
	return re.findall(r'\d+\.\d+', path)[0]

def file2pol(path):
	'''
	pulls polarization from filename using regex

	Parameters
	----------
	path | str: path of file

	Returns
	-------
	str: polarization
	'''
	return re.findall(r'\.(.{2})\.', path)[0]

def add_files_to_distill(source_paths):
	'''
	adds files to paperdistiller database

	Parameters
	----------
	source_paths | list[str]: list of file paths
	'''
	#connect to the database
	dbi = ddbi.DataBaseInterface()

	#check that all files exist
	for source_path in source_paths:
		assert(source_path.startswith('/'))
		assert(os.path.exists(source_path))

	#now run through all the files and build the relevant information for the db
	# get the pols
	pols = [file2pol(source_path) for source_path in source_paths]
	jds = n.array([file2jd(source_path) for source_path in source_paths])
	nights = list(set(jds.astype(n.int)))

	jds_onepol = n.sort([jd for i, jd in enumerate(jds) if pols[i] == pols[0] and jd.astype(int) == nights[0]])
	djd = n.mean(n.diff(jds_onepol))
	print('setting length to ', djd, ' days')

	pols = list(set(pols))#these are the pols I have to iterate over
	print('found the following pols', pols)
	print('found the following nights', nights)

	for night in nights:
		print('adding night', night)
		obsinfo = []
		nightfiles = [source_path for source_path in source_paths if int(float(file2jd(source_path))) == night]
		print(len(nightfiles))
		for pol in pols:
			#filter off all pols but the one I'm currently working on
			files = sorted([source_path for source_path in nightfiles if file2pol(source_path) == pol])
			with dbi.session_scope() as s:
				for i, source_path in enumerate(files):
					try:
						dbi.get_entry(ddbi, s, 'observation', uv_data.jdpol2obsnum(float(file2jd(source_path)), file2pol(source_path), djd))
						print(source_path, 'found in db, skipping')
					except:
						obsinfo.append({'julian_date': float(file2jd(source_path)),
										'pol': file2pol(source_path),
										'host': socket.gethostname(),
										'filename': source_path,
										'length': djd}) #note the db likes jd for all time units

		for i, obs in enumerate(obsinfo):
			if i != 0:
				if n.abs(obsinfo[i - 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
					obsinfo[i].update({'neighbor_low': obsinfo[i - 1]['julian_date']})
			if i != len(obsinfo) - 1:
				if n.abs(obsinfo[i + 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
					obsinfo[i].update({'neighbor_high': obsinfo[i + 1]['julian_date']})

		print('adding {obs_len} observations to the still db'.format(obs_len=len(obsinfo)))

		try:
			with dbi.session_scope() as s:
				for info_dict in obs_info:
					dbi.add_entry_dict(ddbi, s, 'observation', info_dict)
				#dbi.add_observations(obsinfo)
		except:
			print('problem!')
	print('done')

if __name__ == '__main__':
	if len(sys.argv) == 2:
		path_str = sys.argv[1]
	else:
		path_str = raw_input('Input [wildcard included] paths for observations to add: ')
	source_paths = glob.glob(path_str)
	add_files_to_distill(source_paths)
