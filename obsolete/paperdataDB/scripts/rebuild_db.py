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
import os
import paramiko

### Script to rebuild paperdata database
### Finds time and date and writes table into .psv file

### Author: Immanuel Washington
### Date: 05-06-15

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(host, username=username, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
	except:
		try:
			ssh.connect(host, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
		except:
			return None

	return ssh

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
	#DEFAULT VALUE
	size = 0
	if named_host == host:
		size = round(float(sizeof_fmt(get_size(full_path))), 1)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		size_bytes = sftp.stat(full_path).st_size
		size = round(float(sizeof_fmt(size_bytes)), 1)
		sftp.close()
		ssh.close()

	return size

def calc_md5sum(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	md5 = 'NULL'
	if named_host == host:
		md5 = md5sum(full_path)
	else:
		ssh = login_ssh(host)
		sftp = ssh.open_sftp()
		remote_path = sftp.file(full_path, mode='r')
		md5 = remote_path.check('md5', block_size=65536)
		sftp.close()
		ssh.close()

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
	named_host = socket.gethostname()
	if named_host == host:
		try:
			uv = A.miriad.UV(full_path)
		except:
			return None

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
			return None

		if n_times > 1:
			delta_time = -(time_start - time_end)/(n_times - 1)
		else:
			delta_time = -(time_start - time_end)/(n_times)
	else:
		ssh = login_ssh(host)
		time_data_script = '/home/{0}/scripts/paperdata/paper/scripts/paperdata_dbi/scripts/time_data.py'.format('immwa')
		sftp = ssh.open_sftp()
		moved_script = '/home/{0}/scripts/time_data.py'.format('immwa')
		try:
			filestat = sftp.stat(time_data_script)
		except(IOError):
			try:
				filestat = sftp.stat(moved_script)
			except(IOError):
				sftp.put(time_data_script, moved_script)
		sftp.close()
		stdin, time_data, stderr = ssh.exec_command('python {0} {1} {2}'.format(moved_script, host, full_path))

		time_start, time_end, delta_time = [float(info) for info in time_data.read().split(',')]
		ssh.close()

	times = (time_start, time_end, delta_time)
	return times

### Backup Functions

def backup_observations(dbnum, time_date):
	print dbnum
	resultFile = open(dbnum,'wb')
	resultFile.close()

	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('SELECT obsnum, julian_date, polarization, julian_day, era, era_type, data_length FROM paperdata group by obsnum order by obsnum asc')
	res1 = cursor.fetchall()
	#reset polarization
	res1 = tuple((i[0], i[1], i[2], i[3], i[4], i[5], i[6]) if int(i[3]) > 6100 else (i[0], i[1], 'all', i[3], i[4], i[5], i[6]) for i in res1)
	#need time_start, time_end, delta_time, prev_obs, next_obs functions
	cursor.execute('''SELECT SUBSTRING_INDEX(raw_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/', -1), SUBSTRING_INDEX(raw_path, '.', -1), obsnum FROM paperdata where raw_path != 'NULL' group by obsnum order by obsnum asc''')
	res = cursor.fetchall()
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
	#edge
	cursor.execute('''SELECT edge, comments FROM paperdata where raw_path != 'NULL' group by obsnum order by obsnum asc''')
	res3 = cursor.fetchall()
	res3 = tuple((bool(i[0]),) for i in res3)
	resu = tuple(set(zip(res1, res2, res3)))

	resultFile = open(dbnum,'ab')
	wr = csv.writer(resultFile, delimiter='|', lineterminator='\n', dialect='excel')

	for item in resu:
		print item
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

	#for dbnum in (dbnum2, dbnum3, dbnum4, dbnum5):
	for dbnum in (dbnum2,):
		results = ()
		resultFile = open(dbnum,'wb')
		resultFile.close()

		if dbnum == dbnum2:
			#host, path, filename, filetype, full_path, obsnum, filesize, md5sum, tape_index
			cursor.execute('''SELECT 'folio', raw_path, CONCAT('zen.', julian_date, '.uv'), 'uv', CONCAT('folio', ':', raw_path, '/', 'zen.', julian_date, '.uv'), obsnum, raw_file_size_MB, md5sum, tape_index FROM paperdata where raw_path != 'NULL' and raw_path = 'ON TAPE' order by julian_date asc, polarization asc''')
			res1 = cursor.fetchall()
			cursor.execute('''SELECT SUBSTRING_INDEX(raw_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/', -1), SUBSTRING_INDEX(raw_path, '.', -1), raw_path, obsnum, raw_file_size_MB, md5sum, tape_index FROM paperdata where raw_path != 'NULL' and raw_path != 'ON TAPE' and raw_path not like '%ON TAPE%' group by raw_path order by julian_date asc, polarization asc''')
			res2 = cursor.fetchall()
			cursor.execute('''SELECT SUBSTRING_INDEX(raw_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':/', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(raw_path, ':', -1), '/', -1), SUBSTRING_INDEX(raw_path, '.', -1), raw_path, obsnum, raw_file_size_MB, md5sum, tape_index FROM paperdata where raw_path != 'NULL' and raw_path != 'ON TAPE' and raw_path like '%ON TAPE%' group by raw_path order by julian_date asc, polarization asc''')
			res2_2 = cursor.fetchall()
			res = res1 + res2 + res2_2
			resA = {}
			for key, value in enumerate(res):
				resA[key] = value
			#
			#write_to_tape, delete_file
			cursor.execute('''SELECT write_to_tape, delete_file FROM paperdata where raw_path != 'NULL' and raw_path != 'ON TAPE' group by raw_path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			cursor.execute('''SELECT write_to_tape, delete_file FROM paperdata where raw_path != 'NULL' and raw_path = 'ON TAPE' order by julian_date asc, polarization asc''')
			res4 = cursor.fetchall()
			res5 = res3 + res4
			resB = {} 
			res6 = tuple((bool(int(i[0])), bool(int(i[1]))) for i in res5)
			for key, value in enumerate(res6):
				resB[key] = value

			print len(resA.items())
			print len(resB.items())
			resu = tuple(resA[key] + resB[key] for key, value in enumerate(res))
			for item in resu:
				if results is ():
					results = (item,)
				else:
					results += (item,)
				print item

		elif dbnum == dbnum3:
			#host, path, filename, filetype, full_path, obsnum, filesize, md5sum
			cursor.execute('''SELECT SUBSTRING_INDEX(path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(path, ':', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(path, ':', -1), '/', -1), SUBSTRING_INDEX(path, '.', -1), path, obsnum, compr_file_size_MB, compr_md5sum FROM paperdata where path != 'NULL' and compr_md5sum != 'NULL' group by path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need tape_index, write_to_tape, delete_file
			resu = tuple(item + ('NULL', False, False) for item in res)
			#
			for item in resu:
				if results is ():
					results = (item,)
				else:
					results += (item,)
				print item

		elif dbnum == dbnum4:
			#host, npz_path, filename, filetype, full_path, obsnum, filename, md5sum
			cursor.execute('''SELECT SUBSTRING_INDEX(npz_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(npz_path, ':', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(npz_path, ':', -1), '/', -1), SUBSTRING_INDEX(npz_path, '.', -1), npz_path, obsnum, npz_file_size_MB, npz_md5sum FROM paperdata where npz_path != 'NULL' and npz_md5sum != 'NULL' group by npz_path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need tape_index, write_to_tape, delete_file
			resu = tuple(item + ('NULL', False, False) for item in res)
			#
			for item in resu:
				if results is ():
					results = (item,)
				else:
					results += (item,)
				print item
		"""
		elif dbnum == dbnum5:
			#host, final_product_path, filename, filetype, full_path, obsnum
			cursor.execute('''SELECT SUBSTRING_INDEX(final_product_path, ':', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(final_product_path, ':', -1), '/z', 1), SUBSTRING_INDEX(SUBSTRING_INDEX(final_product_path, ':', -1), '/', -1), SUBSTRING_INDEX(final_product_path, '.', -1), final_product_path, obsnum FROM paperdata where final_product_path != 'NULL' group by final_product_path order by julian_date asc, polarization asc''')
			res = cursor.fetchall()
			res1 = res
			#need filesize, md5sum, tape_index
			res2 = []
			for item in res:
				host = item[0]
				path = item[1]
				filename = item[2]
				obsnum = item[5]

				res2.append((calc_size(host, name, filename), calc_md5sum(host, path, filename), 'NULL'))
			#convert back to tuple
			res2 = tuple(res2)
			#
			#write_to_tape, delete_file
			cursor.execute('''SELECT write_to_tape, delete_file FROM paperdata where final_product_path != 'NULL' group by final_product_path order by julian_date asc, polarization asc''')
			res3 = cursor.fetchall()
			#res3 = tuple((bool(i[0]), bool(i[1]))) for i in res3)
			res3 = tuple((False, False) for i in res3)
			resu = zip(res1, res2, res3)
			for item in resu:
				if len(item) >= 2 and type(item[0]) is tuple:
					if results is ():
						results = (tuple(i for i in item),)
					else:
						results += tuple(i for i in item)

		"""
		resultFile = open(dbnum,'ab')
		wr = csv.writer(resultFile, delimiter='|', lineterminator='\n', dialect='excel')

		print results
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
	dbnum1 = '/home/immwa/papertest/backups/paperdata_obs_backup_%s.psv'%(time_date)
	dbnum2 = '/home/immwa/papertest/backups/paperdata_file_raw_backup_%s.psv'%(time_date)
	dbnum3 = '/home/immwa/papertest/backups/paperdata_file_compressed_backup_%s.psv'%(time_date)
	dbnum4 = '/home/immwa/papertest/backups/paperdata_file_npz_backup_%s.psv'%(time_date)
	dbnum5 = '/home/immwa/papertest/backups/paperdata_file_final_backup_%s.psv'%(time_date)
	#backup_observations(dbnum1, time_date)
	backup_files(dbnum2, dbnum3, dbnum4, dbnum5, time_date)
