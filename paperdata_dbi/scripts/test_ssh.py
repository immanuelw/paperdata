import paramiko
import os
import sys
import glob
import socket
import aipy as A
import hashlib

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

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username='immwa'):
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
		uv_data_script = './uv_data.py'
		sftp = ssh.open_sftp()
		try:
			filestat = sftp.stat('./uv_data.py')
		except(IOError):
			sftp.put(uv_data_script, './')
		sftp.close()
		stdin, uv_data, stderr = ssh.exec_command('python {0} {1} {2}'.format(uv_data_script, host, full_path))
		time_start, time_end, delta_time = [float(info) for info in uv_data.read().split(',')]
		ssh.close()

	times = (time_start, time_end, delta_time)
	return times

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
		try:
			remote_path = sftp.file(full_path, mode='r')
			md5 = remote_path.check('md5', block_size=65536)
		except(IOError):
			#remote_path_2 =  sftp.file('{0}/visdata'.format(full_path), mode='r')
			#md5 = remote_path_2.check('md5', block_size=65536)
			stdin, md5, stderr = ssh.exec_command('md5sum {0}/visdata'.format(full_path))
			
		sftp.close()
		ssh.close()

	return md5.read()

if __name__ == '__main__':	
	if len(sys.argv) == 2:
		input_host = sys.argv[1].split(':')[0]
		if input_host == sys.argv[1]:
			print 'Needs host'
			sys.exit()
		input_paths = glob.glob(sys.argv[1].split(':')[1])
	elif len(sys.argv) == 3:
		input_host = sys.argv[1]
		input_paths = glob.glob(sys.argv[2])
	else:
		input_host = raw_input('Source directory host: ')
		input_paths = glob.glob(raw_input('Source directory path: '))

	for input_path in input_paths:
		path = os.path.dirname(input_path)
		filename = os.path.basename(input_path)
		print input_path
		print calc_md5sum(input_host, path, filename)
		print calc_times(input_host, path, filename)
