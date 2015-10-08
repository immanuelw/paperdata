#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paper

from __future__ import print_function
import os
import hashlib
import socket
import paper as ppdata

### Module to add files to paper database
### Adds files using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def get_size(start_path):
	'''
	output byte size of directory or file

	Parameters
	----------
	start_path | str: path of directory or file

	Returns
	-------
	int: amount of bytes
	'''
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)

	return total_size

def sizeof_fmt(num):
	'''
	converts bytes to MB

	Parameters
	----------
	num | int: amount of bytes

	Returns
	-------
	float: amount of MB to 1 decimal place
	'''
	for byte_size in ('KB', 'MB'):
		num /= 1024.0

	return round(num, 1)

def calc_size(host, path, filename):
	'''
	calculates size of directory or file on any host
	logins into host if necessary

	Parameters
	----------
	host | str: host of file
	full_path | str: full path of directory or file

	Returns
	-------
	float: size of directory or file in MB
	'''
	named_host = socket.gethostname()
	if named_host == host:
		size = sizeof_fmt(get_size(full_path))
	else:
		with ppdata.ssh_scope(host) as ssh:
			with ssh.open_sftp() as sftp:
				size_bytes = sftp.stat(full_path).st_size
				size = sizeof_fmt(size_bytes)

	return size

def get_md5sum(fname):
	'''
	calculate the md5 checksum of a file whose filename entry is fname.

	Parameters
	----------
	fname | str: path of directory or file

	Returns
	-------
	str: 32-bit hex integer md5 checksum
	'''
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
	'''
	calculates md5 checksum of directory or file on any host
	logins into host if necessary

	Parameters
	----------
	host | str: host of file
	full_path | str: full path of directory or file

	Returns
	-------
	str: md5 checksum
	'''
	named_host = socket.gethostname()
	if named_host == host:
		md5 = get_md5sum(full_path)
	else:
		with ppdata.ssh_scope(host) as ssh:
			try:
				with ssh.open_sftp() as sftp:
					remote_path = sftp.file(full_path, mode='r')
					md5 = remote_path.check('md5', block_size=65536)
			except(IOError):
				vis_path = os.path.join(full_path, 'visdata')
				_, md5_out, _ = ssh.exec_command('md5sum {vis_path}'.format(vis_path=vis_path))
				md5 = md5_out.read().split(' ')[0]

	return md5

def file_names(full_path):
	'''
	separates full path of directory or file into parts

	Parameters
	----------
	full_path | str: full path of directory or file

	Returns
	-------
	tuple:
		str: partial path,
		str: directory/file name
		str: extension/filetype
	'''
	path = os.path.dirname(full_path)
	filename = os.path.basename(full_path)
	filetype = filename.split('.')[-1]

	return path, filename, filetype

if __name__ == '__main__':
	print('Not a script file, just a module')
