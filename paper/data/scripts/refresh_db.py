'''
paper.data.scripts.refresh_db

refreshes database by chceking file paths for directories and files

author | Immanuel Washington

Functions
---------
path_exists | checks for path existence and returns boolean
refresh_db | converts json file into python dicts, then loads into paperdata database
'''
from __future__ import print_function
import socket
import paper as ppdata
from paper.data import dbi as pdbi

def path_exists(sftp, path):
	'''
	checks for path existence

	Parameters
	----------
	sftp | object: SFTP object
	path | str: path of file or directory

	bool: path exists
	'''
	try:
		sftp.stat(path)
		return True
	except IOError:
		return False

def refresh_db(dbi):
	'''
	fixes database files and directories that have been moved/deleted

	Parameters
	----------
	dbi | object: database interface object,
	'''
	source_host = socket.gethostname()
	table = pdbi.File
	hosts = ('folio', 'node16', 'nas1', 'nas2', 'pot1', 'pot2', 'pot3', 'pot4', 'pot8')
	with dbi.session_scope() as s:
		for host in hosts:
			FILEs = s.query(table).filter(table.host == host).all()
			if source_host == host:
				for FILE in FILEs:
					if not os.path.exists(FILE.source):
						s.delete(FILE)
			else:
				for FILE in FILEs:
					with ppdata.ssh_scope(host) as ssh:
						with ssh.open_sftp() as sftp:
							if not path_exists(sftp, FILE.source):
								s.delete(FILE)

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	refresh_db(dbi)
