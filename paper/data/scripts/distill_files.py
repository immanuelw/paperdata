#!/usr/global/paper/CanopyVirtualEnvs/PAPER_Distiller/bin/python
'''
Input a list of files and insert into the db.  The files must exist and be findable on the filesystem
NB filenames must be FULL PATH. If the root is not '/' for all files it will exit

KEY NOTE: Assumes all files are contiguous.  I sort the files by jd and then match up neighboring pols as neighbors for the
   ddr algorithm
'''
from __future__ import print_function
import os
import sys
import glob
import re
import socket
import numpy as n
import ddr_compress.dbi as ddbi 
from paper.data import uv_data

def file2jd(full_path):
	'''
	pulls julian date from filename

	Parameters
	----------
	full_path | str: full path of file

	Returns
	-------
	float(5): julian date
	'''
	return re.findall(r'\d+\.\d+', full_path)[0]

def file2pol(full_path):
	'''
	pulls polarization from filename

	Parameters
	----------
	full_path | str: full path of file

	Returns
	-------
	str: polarization
	'''
	return re.findall(r'\.(.{2})\.', full_path)[0]

def add_files_to_distill(input_paths):
	'''
	adds files to paperdistiller database

	Parameters
	----------
	zenuv | str: full path of file
	'''
	#connect to the database
	dbi = ddbi.DataBaseInterface()

	#check that all files exist
	for input_path in input_paths:
		assert(input_path.startswith('/'))
		assert(os.path.exists(input_path))

	#now run through all the files and build the relevant information for the db
	# get the pols
	pols = [file2pol(input_path) for input_path in input_paths]
	jds = n.array([file2jd(input_path) for input_path in input_paths])
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
		nightfiles = [input_path for input_path in input_paths if int(float(file2jd(input_path))) == night]
		print(len(nightfiles))
		for pol in pols:
			#filter off all pols but the one I'm currently working on
			files = sorted([input_path for input_path in nightfiles if file2pol(input_path) == pol])
			for i, input_path in enumerate(files):
				try:
					dbi.get_obs(uv_data.jdpol2obsnum(float(file2jd(input_path)), file2pol(input_path), djd))
					print(filename, 'found in db, skipping')
				except:
					obsinfo.append({'julian_date': float(file2jd(input_path)),
									'pol': file2pol(input_path),
									'host': socket.gethostname(),
									'filename': input_path,
									'length': djd}) #note the db likes jd for all time units

		for i, obs in enumerate(obsinfo):
			filename = obs['filename']
			if i != 0:
				if n.abs(obsinfo[i - 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
					obsinfo[i].update({'neighbor_low': obsinfo[i - 1]['julian_date']})
			if i != len(obsinfo) - 1:
				if n.abs(obsinfo[i + 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
					obsinfo[i].update({'neighbor_high': obsinfo[i + 1]['julian_date']})

		print('adding {obs_len} observations to the still db'.format(obs_len=len(obsinfo)))

		try:
			dbi.add_observations(obsinfo)
		except:
			print('problem!')
			#dbi.test_db()
	print('done')

if __name__ == '__main__':
	path_str = sys.argv[1]
	input_paths = glob.glob(path_str)
	add_files_to_distill(input_paths)
