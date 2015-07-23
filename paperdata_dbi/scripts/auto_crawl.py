#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdata

import sys
import aipy as A
import hashlib
import glob
import socket
import os
import csv
import MySQLdb

### Script to add files to paperdata database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

#Functions which simply find the file size
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['KB', 'MB']:
		num /= 1024.0
	return "%3.1f" % (num)

### other functions

#create function to uniquely identify files
def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = A.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

def md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(fname, 'rb')
	except(IOError):
		afile = open("%s/visdata"%fname, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)
	return hasher.hexdigest()

def calc_obs_data(host, full_path):
	#mostly file data
	host = host
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)
	filetype = filename.split('.')[-1]

	if os.path.isdir(full_path) or os.path.isfile(full_path):
		pass
	else:
		print full_path + '|' + 'No Longer Exists'
		return None

	if full_path in ('/data3/data1/paper/psa/psa228/zen.2440587.69719.uv', '/data3/data1/paper/psa/psa228/zen.2455228.00925.uv',
					'/data3/paper/jkris/560x/5600/zen.2455600.20624.uv', '/data3/paper/jkris/560x/5600/zen.2455600.22016.uv',
					'/data3/paper/psa/Dec2010/zen.2455537.71473.uv', '/data4/paper/128_sim_data/sim4/zen.2456892.48252.xx.uv',
					'/data4/paper/2013EoR/2456644/zen.2456644.18779.xy.uv', '/data4/paper/2013EoR/2456644/zen.2456644.19475.xx.uv',
					'/data4/paper/2013EoR/2456710/zen.2456710.19470.yx.uvcRRE', '/data4/paper/2013EoR/2456711/zen.2456711.39657.yx.uvcRRE',
					'/data4/paper/2013EoR/2456711/zen.2456711.50097.xy.uvcRRE', '/data4/paper/2013EoR/2456711/zen.2456711.58448.xy.uvcRRE',
					'/data4/paper/2013EoR/2456713/zen.2456713.25039.xy.uvcRRE', '/data4/paper/2013EoR/2456713/zen.2456713.30607.xx.uvcRRE',
					'/data4/paper/2013EoR/2456713/zen.2456713.50093.xy.uvcRRE', '/data4/paper/2013EoR/2456730/zen.2456730.22261.xx.uvcRRE',
					'/data4/paper/2014EoR/pot2/data1/2456970/zen.2456970.41051.yx.uvcRRE',
					'/data4/paper/dcj/psa128/calibration/zen.2456660.37569.yy.uv',
					'/data4/raw_data/Dec2011/psa5842/raw_zenuv/zen.2455846.81133.uv',
					'/data4/raw_data/EoR2013/psa6622/zen.2456622.20864.xx.uvcRRE',
					'/data4/raw_data/EoR2013/psa6661/zen.2456661.55668.yx.uvcRRE'):
		obsnum = 0
		filesize = sizeof_fmt(get_size(full_path))
		try:
			md5 = md5sum(full_path)
		except(IOError):
			return None
		tape_index = None

		write_to_tape = False
		delete_file = False

		file_data = (host, path, filename, filetype, full_path, obsnum, filesize, md5, tape_index, write_to_tape, delete_file)

		return file_data

	#Dictionary of polarizations
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

	#allows uv access
	named_host = socket.gethostname()
	if filetype in ('uv', 'uvcRRE'):
		if named_host == host:
			try:
				uv = A.miriad.UV(full_path)
			except:
				obsnum = 0
				filesize = sizeof_fmt(get_size(full_path))
				try:
					md5 = md5sum(full_path)
				except(IOError):
					return None
				tape_index = None

				write_to_tape = False
				delete_file = False

				file_data = (host, path, filename, filetype, full_path, obsnum, filesize, md5, tape_index, write_to_tape, delete_file)

				return file_data

			#indicates julian date
			julian_date = round(uv['time'], 5)

			#assign letters to each polarization
			if uv['npol'] == 1:
				polarization = pol_dict[uv['pol']]
			elif uv['npol'] == 4:
				polarization = 'all'

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
				obsnum = 0
				filesize = sizeof_fmt(get_size(full_path))
				try:
					md5 = md5sum(full_path)
				except(IOError):
					return None
				tape_index = None

				write_to_tape = False
				delete_file = False

				file_data = (host, path, filename, filetype, full_path, obsnum, filesize, md5, tape_index, write_to_tape, delete_file)

				return file_data

			if n_times > 1:
				delta_time = -(time_start - time_end)/(n_times - 1)
			else:
				delta_time = -(time_start - time_end)/(n_times)

			length = round(n_times * delta_time, 5)

			#gives each file unique id
			if length > 0:
				if polarization == 'all':
					obsnum = jdpol2obsnum(julian_date, 'yy', length)
				else:
					obsnum = jdpol2obsnum(julian_date, polarization, length)
			else:
				obsnum = 0

			filesize = sizeof_fmt(get_size(full_path))

	elif filetype in ('npz',):
		obsnum = 0
		filesize = round(os.path.getsize(full_path) / 1024.0 / 1024.0, 1)

	try:
		md5 = md5sum(full_path)
	except(IOError):
		return None
	tape_index = None

	write_to_tape = False
	delete_file = False

	file_data = (host, path, filename, filetype, full_path, obsnum, filesize, md5, tape_index, write_to_tape, delete_file)

	return file_data

def dupe_check(input_paths):
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()
	cursor.execute('''SELECT raw_path FROM paperdata where raw_path != 'NULL' order by julian_date asc, polarization asc''')
	resA = cursor.fetchall()

	cursor.execute('''SELECT path FROM paperdata where path != 'NULL' order by julian_date asc, polarization asc''')
	resB = cursor.fetchall()

	cursor.execute('''SELECT npz_path FROM paperdata where npz_path != 'NULL' order by julian_date asc, polarization asc''')
	resC = cursor.fetchall()

	cursor.close()
	connection.close()

	resAX = tuple(res[0] for res in resA)
	resBX = tuple(res[0] for res in resB)
	resCX = tuple(res[0] for res in resC)

	filenames = resAX + resBX + resCX

	#for each input file, check if in filenames
	unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
		
	return unique_paths

def write_data(input_host, input_paths, data_f):

	for input_path in input_paths:
		item = calc_obs_data(input_host, input_path)
		if item is None:
			continue
		print item
		data_file = open(data_f, 'ab')
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')
		wr.writerow(item)
		data_file.close()

	return None

def file_check():
	input_paths = []
	npz_paths = []
	host = 'folio:'
	for root, dirs, files in os.walk('/data3/paper'):
		for direc in dirs:
			if direc.startswith('zen') and direc.endswith(('uv', 'uvcRRE')):
					input_paths.append(host + os.path.join(root, direc))
					print direc
		for file_path in files:
			if file_path.startswith('zen') and file_path.endswith('npz') and 'uvcRE' in file_path:
				npz_paths.append(host + os.path.join(root, file_path))
				print file_path

	input_paths += npz_paths

	return input_paths

def write_file(input_paths):
	file_store = '/home/immwa/missed_file_list.psv'
	data_X = open(file_store, 'ab')
	wrX = csv.writer(data_X, delimiter='|', lineterminator='\n', dialect='excel')
	
	for input_path in input_paths:
		wrX.writerow([input_path])
	data_X.close()

	return None

def read_file():
	#file_store = '/home/immwa/missed_file_list.psv'
	file_store = '/home/immwa/files_missed.psv'
	data_X = open(file_store, 'rb')
	readX = csv.reader(data_X, delimiter='|', lineterminator='\n', dialect='excel')

	input_paths = []
	for row in readX:
		input_paths.append(row[0])

	data_X.close()

	return input_paths

if __name__ == '__main__':
	input_host = socket.gethostname()

	#input_paths = file_check()
	#write_file(input_paths)

	input_paths = read_file()
	#input_paths = dupe_check(input_paths)
	#input_paths = tuple(sorted(input_paths))
	#for input_path in input_paths:
	#	print input_path
 
	data_f = '/home/immwa/missed_paper.psv'
	input_paths = tuple(input_path.split(':')[1] for input_path in input_paths)
	write_data(input_host, input_paths, data_f)
