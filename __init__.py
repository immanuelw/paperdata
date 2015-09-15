import os
import paramiko

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	key_filename = os.path.expanduser('~/.ssh/id_rsa')
	try:
		ssh.connect(host, username=username, key_filename=key_filename)
	except:
		try:
			ssh.connect(host, key_filename=key_filename)
		except:
			return None

	return ssh
