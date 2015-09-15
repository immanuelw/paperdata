#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdata

from __future__ import print_function
import os
import hashlib
import socket
import paperdata as ppdata

### Module to add files to paperdata database
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
	#converts bytes to MB
	for byte_size in ('KB', 'MB'):
		num /= 1024.0
	return round(num, 1)

### other functions
def calc_size(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	size = 0
	if named_host == host:
		size = sizeof_fmt(get_size(full_path))
	else:
		ssh = ppdata.login_ssh(host)
		sftp = ssh.open_sftp()
		size_bytes = sftp.stat(full_path).st_size
		size = sizeof_fmt(size_bytes)
		sftp.close()
		ssh.close()

	return size

def get_md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(fname, 'rb')
	except(IOError):
		fname = os.path.join(fname, 'visdata')
		afile = open(fname, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)
	return hasher.hexdigest()

def calc_md5sum(host, path, filename):
	named_host = socket.gethostname()
	full_path = os.path.join(path, filename)
	#DEFAULT VALUE
	md5 = None
	if named_host == host:
		md5 = get_md5sum(full_path)
	else:
		ssh = ppdata.login_ssh(host)
		try:
			sftp = ssh.open_sftp()
			remote_path = sftp.file(full_path, mode='r')
			md5 = remote_path.check('md5', block_size=65536)
			sftp.close()
		except(IOError):
			vis_path = os.path.join(full_path, 'visdata')
			stdin, md5_out, stderr = ssh.exec_command('md5sum {vis_path}'.format(vis_path=vis_path))
			md5 = md5_out.read().split(' ')[0]
		ssh.close()

	return md5

def file_names(full_path):
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)
	filetype = filename.split('.')[-1]
	return path, filename, filetype

if __name__ == '__main__':
	print('Not a script file, just a module')
