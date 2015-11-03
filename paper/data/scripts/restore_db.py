'''
paper.data.scripts.restore_db

restores paperdata database from backup json file

author | Immanuel Washington

Functions
---------
restore_db | converts json file into python dicts, then loads into paperdata database
'''
from __future__ import print_function
import sys
import glob
import json
from paper.data import dbi as pdbi
import add_files
import sqlalchemy.exc

def restore_db(dbi, backup_file=None, table=None):
	'''
	loads backups from json into database

	Parameters
	----------
	dbi | object: database interface object,
	backup_file | str: name of backup file --defaults to None
	table | str: table name --defaults to None
	'''
	if table is None:
		return
	if backup_file is None:
		backup_list = sorted(glob.glob('/data4/paper/paperdata_backup/[0-9]*'), reverse=True)
		timestamp = int(backup_list[0].split('/')[-1])
		backup_file = '/data4/paper/paperdata_backup/{timestamp}/{table}_{timestamp}.json'.format(table=table, timestamp=timestamp)

	with dbi.session_scope() as s, open(backup, 'r') as backup_db:
		entry_list = json.load(backup_db)
		for entry_dict in entry_list:
			print(entry_dict.items())
			try:
				dbi.add_entry_dict(s, table, entry_dict)
			except KeyboardInterrupt:
				raise
			except:
				print('Failed to load in entry')

if __name__ == '__main__':
	dbi = pdbi.DataBaseInterface()
	if len(sys.argv) == 3:
		backup_table = sys.argv[1]
		backup_file = sys.argv[2]
		restore_db(dbi, backup_file, table=backup_table)
	else:
		#restore_db(dbi, table='Observation')
		restore_db(dbi, table='File')
		#restore_db(dbi, table='Feed')
		#restore_db(dbi, table='Log')
		#restore_db(dbi, table='RTPFile')
	add_files.update_obsnums(dbi)
	add_files.connect_observations(dbi)
	#add_files.update_md5(dbi)
