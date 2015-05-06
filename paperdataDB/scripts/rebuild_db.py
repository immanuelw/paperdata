#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import time
import csv
import subprocess
import aipy as A
import hashlib
import glob
import socket

### Script to rebuild paperdata database
### Finds time and date and writes table into .psv file

### Author: Immanuel Washington
### Date: 05-06-15

#Functions which simply find the file size
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['bytes','KB','MB']:
		if num < 1024.0:
			return "%3.1f" % (num)
		num /= 1024.0
	num *= 1024.0
	return "%3.1f" % (num)

### other functions

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

def calc_size(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	if named_host == host:
		size = round(float(sizeof_fmt(get_size(full_path))), 1)
	#else:
		##SSH INTO CORRECT HOST -- PARAMIKO??
			#size = round(float(sizeof_fmt(get_size(full_path))), 1)
		###EXIT??

	return size

def calc_md5sum(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	if named_host == host:
		md5 = md5sum(full_path)
	#else:
		##SSH INTO CORRECT HOST -- PARAMIKO??
			#size = md5sum(full_path)
		###EXIT??

	return md5

def get_prev_obs(obsnum):
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT obsnum FROM paperdata where obsnum = {0} group by obsnum order by obsnum asc limit 1'''.format(int(obsnum) - 1))
	results = cursor.fetchall()
	cursor.close()
	connection.close()

	if len(results) == 0:
		prev_obs = 'NULL' #XXXX --- should it be 0 or maybe empty?
	else:
		prev_obs = int(results[0][0])

	return prev_obs

def get_next_obs(obsnum):
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT obsnum FROM paperdata where obsnum = {0} group by obsnum order by obsnum asc limit 1'''.format(int(obsnum) + 1))
	results = cursor.fetchall()
	cursor.close()
	connection.close()

	if len(results) == 0:
		next_obs = 'NULL' #XXXX --- should it be 0 or maybe empty?
	else:
		next_obs = int(results[0][0])

	return next_obs

def calc_times(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	times =  ('NULL', 'NULL', 'NULL')
	if named_host == host:
		#allows uv access
		try:
			uv = A.miriad.UV(full_path)
		except:
			return times

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
			return times

		if n_times > 1:
			delta_time = -(time_start - time_end)/(n_times - 1)
		else:
			delta_time = -(time_start - time_end)/(n_times)

		times = (time_start, time_end, delta_time)
	#else:
		##SSH INTO CORRECT HOST -- PARAMIKO??
			#allows uv access
			#try:
			#	uv = A.miriad.UV(full_path)
			#except:
			#	return times

			#time_start = 0
			#time_end = 0
			#n_times = 0
			#c_time = 0

			#try:
			#	for (uvw, t, (i,j)),d in uv.all():
			#		if time_start == 0 or t < time_start:
			#			#time_start = t
			#		if time_end == 0 or t > time_end:
			#			#time_end = t
			#		if c_time != t:
			#			#c_time = t
			#			#n_times += 1
			#except:
			#	return times

			#if n_times > 1:
			#	delta_time = -(time_start - time_end)/(n_times - 1)
			#else:
			#	delta_time = -(time_start - time_end)/(n_times)

			#times = (time_start, time_end, delta_time)
	
		###EXIT??

	return times

### Backup Functions

def backup_observations(dbnum, time_date):
	print dbnum
	resultFile = open(dbnum,'wb')
	resultFile.close()

	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('SELECT obsnum, julian_date, polarization, julian_day, era, era_type, data_length FROM paperdata group by obsnum order by obsnum asc')
	results = cursor.fetchall()

	resultFile = open(dbnum,'ab')
	wr = csv.writer(resultFile, delimiter='|', lineterminator='\n', dialect='excel')

	for item in results:
		wr.writerow(item)
	resultFile.close()

	print time_date
	print 'Table data backup saved'

	# Close the cursor object
	cursor.close()
	connection.close()

	return None

def backup_files(dbnum2, dbnum3, dbnum4, dbnum5, time_date):
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	for dbnum in (dbnum2, dbnum3, dbnum4, dbnum5):
		results = ()
		resultFile = open(dbnum,'wb')
		resultFile.close()

		if dbnum == dbnum2:
			#host, path, filename, filetype, obsnum, filesize, md5sum, tape_index
			cursor.execute('''SELECT SUBSTRING_INDEX(raw_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/', -1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/z', 1), SUBSTRING_INDEX(raw_path, '.', -1), obsnum, raw_file_size_MB, md5sum, tape_index FROM paperdata where raw_path != 'NULL' group by raw_path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need time_start, time_end, delta_time, prev_obs, next_obs functions
			res2 = []
			for item in res:
				host = item[0]
				path = item[1]
				filename = item[2]
				obsnum = item[4]

				res2.append(calc_times(host, path, filename) + (get_prev_obs(obsnum), get_next_obs(obsnum)))
			#convert back to tuple
			res2 = tuple(res2)
			#
			#edge, write_to_tape, delete_file
			cursor.execute('''SELECT edge, write_to_tape, delete_file FROM paperdata where raw_path != 'NULL' group by raw_path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			#XXXX zip time_start tuple as well zip(res1, res2, res3)
			resu = zip(res1, res2, res3)
			for item in resu:
				if len(item) >= 2 and type(item[0]) is tuple:
					results += tuple(i for i in item)

		elif dbnum == dbnum3:
			#host, path, filename, filetype, obsnum, filesize
			cursor.execute('''SELECT SUBSTRING_INDEX(path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(path, ':', -1), '/', -1), SUBSTRING_INDEX(SUBSTRING_INDEX(path, ':', -1), '/z', 1), SUBSTRING_INDEX(path, '.', -1), obsnum, compr_file_size_MB FROM paperdata where path != 'NULL' group by path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need md5sum, tape_index, time_start, time_end, delta_time, prev_obs, next_obs functions
			res2 = []
			for item in res:
				host = item[0]
				path = item[1]
				filename = item[2]
				obsnum = item[4]

				res2.append((calc_md5sum(host, path, filename), 'NULL') + calc_times(host, path, filename) + (get_prev_obs(obsnum), get_next_obs(obsnum)))
			#convert back to tuple
			res2 = tuple(res2)
			#
			#edge, write_to_tape, delete_file
			cursor.execute('''SELECT edge, write_to_tape, delete_file FROM paperdata where path != 'NULL' group by path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			#XXXX zip time_start tuple as well zip(res1, res2, res3)
			resu = zip(res1, res2, res3)
			for item in resu:
				if len(item) >= 2 and type(item[0]) is tuple:
					results += tuple(i for i in item)

		elif dbnum == dbnum4:
			#host, npz_path, filename, filetype, obsnum
			cursor.execute('''SELECT SUBSTRING_INDEX(npz_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(npz_path, ':', -1), '/', -1), SUBSTRING_INDEX(SUBSTRING_INDEX(npz_path, ':', -1), '/z', 1), SUBSTRING_INDEX(npz_path, '.', -1), obsnum FROM paperdata where npz_path != 'NULL' group by npz_path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need filesize, md5sum, tape_index, time_start, time_end, delta_time, prev_obs, next_obs functions
			res2 = []
			for item in res:
				host = item[0]
				path = item[1]
				filename = item[2]
				obsnum = item[4]

				res2.append((calc_size(host, name, filename), calc_md5sum(host, path, filename), 'NULL') + calc_times(host, path, filename) + (get_prev_obs(obsnum), get_next_obs(obsnum)))
			#convert back to tuple
			res2 = tuple(res2)
			#
			#edge, write_to_tape, delete_file
			cursor.execute('''SELECT edge, write_to_tape, delete_file FROM paperdata where npz_path != 'NULL' group by npz_path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			#XXXX zip time_start tuple as well zip(res1, res2, res3)
			resu = zip(res1, res2, res3)
			for item in resu:
				if len(item) >= 2 and type(item[0]) is tuple:
					results += tuple(i for i in item)

		elif dbnum == dbnum5:
			#host, final_product_path, filename, filetype, obsnum
			cursor.execute('''SELECT SUBSTRING_INDEX(final_product_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(final_product_path, ':', -1), '/', -1), SUBSTRING_INDEX(SUBSTRING_INDEX(final_product_path, ':', -1), '/z', 1), SUBSTRING_INDEX(final_product_path, '.', -1), obsnum FROM paperdata where final_product_path != 'NULL' group by final_product_path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need filesize, md5sum, tape_index, time_start, time_end, delta_time, prev_obs, next_obs functions
			res2 = []
			for item in res:
				host = item[0]
				path = item[1]
				filename = item[2]
				obsnum = item[4]

				res2.append((calc_size(host, name, filename), calc_md5sum(host, path, filename), 'NULL') + calc_times(host, path, filename) + (get_prev_obs(obsnum), get_next_obs(obsnum)))
			#convert back to tuple
			res2 = tuple(res2)
			#
			#edge, write_to_tape, delete_file
			cursor.execute('''SELECT edge, write_to_tape, delete_file FROM paperdata where final_product_path != 'NULL' group by final_product_path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			#XXXX zip time_start tuple as well zip(res1, res2, res3)
			resu = zip(res1, res2, res3)
			for item in resu:
				if len(item) >= 2 and type(item[0]) is tuple:
					results += tuple(i for i in item)


		resultFile = open(dbnum,'ab')
		wr = csv.writer(resultFile, delimiter='|', lineterminator='\n', dialect='excel')

		for item in results:
			wr.writerow(item)
		resultFile.close()

	print time_date
	print 'Table data backup saved'

	# Close the cursor object
	cursor.close()
	connection.close()

	return None

if __name__ == '__main__':
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	dbnum1 = '/data2/home/immwa/scripts/paperdata/backups/paperdata_obs_backup_%s.psv'%(time_date)
	dbnum2 = '/data2/home/immwa/scripts/paperdata/backups/paperdata_file_raw_backup_%s.psv'%(time_date)
	dbnum3 = '/data2/home/immwa/scripts/paperdata/backups/paperdata_file_compressed_backup_%s.psv'%(time_date)
	dbnum4 = '/data2/home/immwa/scripts/paperdata/backups/paperdata_file_npz_backup_%s.psv'%(time_date)
	dbnum5 = '/data2/home/immwa/scripts/paperdata/backups/paperdata_file_final_backup_%s.psv'%(time_date)
	backup_observations(dbnum1, time_date)
	backup_files(dbnum2, dbnum3, dbnum4, dbnum5, time_date)
