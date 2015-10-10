from __future__ import print_function
import os
import hashlib
import socket
import paper as ppdata

def get_size(path):
	'''
	output byte size of directory or file

	Parameters
	----------
	path | str: path of directory or file

	Returns
	-------
	int: amount of bytes
	'''
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(path):
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

def calc_size(host, path):
	'''
	calculates size of directory or file on any host
	logins into host if necessary

	Parameters
	----------
	host | str: host of file
	path | str: path of directory or file

	Returns
	-------
	float: size of directory or file in MB
	'''
	named_host = socket.gethostname()
	if named_host == host:
		size = sizeof_fmt(get_size(path))
	else:
		with ppdata.ssh_scope(host) as ssh:
			with ssh.open_sftp() as sftp:
				size_bytes = sftp.stat(path).st_size
				size = sizeof_fmt(size_bytes)

	return size

def get_md5sum(path):
	'''
	calculate the md5 checksum of a uv file

	Parameters
	----------
	path | str: path of directory or file

	Returns
	-------
	str: 32-bit hex integer md5 checksum
	'''
	path = path.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(path, 'rb')
	except(IOError):
		fname = os.path.join(path, 'visdata')
		afile = open(path, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) > 0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)

	return hasher.hexdigest()

def calc_md5sum(host, path):
	'''
	calculates md5 checksum of directory or file on any host
	logins into host if necessary

	Parameters
	----------
	host | str: host of file
	path | str: path of directory or file

	Returns
	-------
	str: md5 checksum
	'''
	named_host = socket.gethostname()
	if named_host == host:
		md5 = get_md5sum(path)
	else:
		with ppdata.ssh_scope(host) as ssh:
			try:
				with ssh.open_sftp() as sftp:
					with sftp.file(path, mode='r') as remote_path:
						md5 = remote_path.check('md5', block_size=65536)
			except(IOError):
				vis_path = os.path.join(path, 'visdata')
				_, md5_out, _ = ssh.exec_command('md5sum {vis_path}'.format(vis_path=vis_path))
				md5 = md5_out.read().split(' ')[0]

	return md5

def file_names(path):
	'''
	separates full path of directory or file into parts

	Parameters
	----------
	path | str: path of directory or file

	Returns
	-------
	tuple:
		str: base path
		str: directory/file name
		str: extension/filetype
	'''
	base_path, filename = os.path.split(path)
	filetype = filename.split('.')[-1]

	return base_path, filename, filetype

if __name__ == '__main__':
	print('Not a script file, just a module')
