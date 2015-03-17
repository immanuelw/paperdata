#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import shutil
import csv
import os

### Script to delete files set by ISUS
### Looks through database for delete_file marker, deletes every directory and file in list

### Author: Immanuel Washington
### Date: 8-20-14

def delete_files(usrnm, pswd, confirm, failed_delete):
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT obsnum, raw_path, tape_index, delete_file from paperdata where delete_file = 1 and tape_index like '%PAPR%PAPR%' order by julian_date asc''')
	results = cursor.fetchall()

	#results is a list of lists
	deletion = []
	for items in results:
		obsnum = int(items[0])
		raw_path = items[1]
		tape_path = items[2]
		delete = int(items[3])
		if delete == 1 and tape_path != 'NULL' and raw_path != 'NULL':
			deletion.append([raw_path,obsnum,tape_path])

	if confirm == 'y':
		for item in deletion:
			del_file = open(failed_delete,'ab')
			fd = csv.writer(del_file, delimiter='|', dialect='excel')
			raw_path = item[0]
			obsnum = int(item[1])
			tape_index = item[2]
			confirmed = raw_input('Are you sure you want to delete %s [tape_index:%s] (y/n) ?: '%(raw_path.split('/')[-1], tape_index))
			if confirmed == 'y':
				try:
					print 'Deleting %s ...'%(raw_path.split(':')[1])
					shutil.rmtree(raw_path.split(':')[1])
				except:
					fd.writerow([raw_path, 'Not removed'])
					print 'ERROR: uv file %s not removed' %(raw_path)
					del_file.close()
					continue
			else:
				fd.writerow([raw_path, 'Not removed by choice'])
				print 'CHOSEN: uv file %s not removed' %(raw_path)
				del_file.close()
				quit = raw_input('Do you want to quit (y/n)?: ')
				if quit == 'y':
					sys.exit()
				continue

			if not os.path.isdir(raw_path):
				try:
					print obsnum
					cursor.execute('''
					UPDATE paperdata
					SET delete_file = 0, raw_path = 'ON TAPE'
					WHERE obsnum = %d and raw_path = '%s';
					'''%(obsnum, raw_path))
					del_file.close()
				except:
					fd.writerow([raw_path, 'Not updated'])
					print 'ERROR: uv file %s not updated -- mysql failure' %(raw_path)
					del_file.close()
					continue
					
			else:
				fd.writerow([raw_path, 'Not updated'])
				print 'ERROR: uv file %s not updated -- file does not exist' %(raw_path)
				del_file.close()
				continue
		print 'Table data updated.'
	else:
		sys.exit()

	# Close and save changes to database
	cursor.close()
	connection.commit()
	connection.close()

	return None

if __name__ == '__main__':
	usrnm = raw_input('Root username: ')
	pswd = getpass.getpass('Root password: ')
	confirm = raw_input('Are you sure you want to delete (y/n) ?: ')
	failed_delete = '/data4/paper/paperoutput/failed_deletion.psv'
	delete_files(usrnm, pswd, confirm, failed_delete)
