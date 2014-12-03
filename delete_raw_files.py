#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import shutil
import csv

### Script to delete files set by ISUS
### Looks through database for delete_file marker, deletes every directory and file in list

### Author: Immanuel Washington
### Date: 8-20-14

def delete_files(usrnm, pswd, confirm, failed_delete):
	del_file = open(failed_delete,'wb')
	del_file.close()

	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('SELECT obsnum, raw_location, tape_location, delete_file from paperdata where delete_file = 1 order by julian_date asc')
	results = cursor.fetchall()

	#results is a list of lists
	deletion = []
	for items in results:
		obsnum = int(items[0])
		raw_path = items[1]
		tape_path = items[2]
		delete = int(items[3])
		if delete == 1 and tape_path != 'NULL' and raw_path != 'NULL':
			deletion.append([raw_path,obsnum])

	if confirm == 'y':
		for item in deletion:
			del_file = open(failed_delete,'ab')
			fd = csv.writer(del_file, delimiter='|', dialect='excel')
			raw_path = item[0]
			obsnum = item[1]
			try:
				shutil.rmtree(item[0])
			except:
				fd.writerow([item[0], 'Not removed'])
				print 'ERROR: uv file %s not removed' %(item[0])
				del_file.close()
				continue

			if not os.path.isdir(item[0]):
				cursor.execute('''
				UPDATE paperdata
				SET delete_file = 0, raw_location = 'ON TAPE'
				WHERE obsnum = %d and raw_location = '%s';
				'''%(obsnum, raw_path))
			else:
				fd.writerow([item[0], 'Not updated'])
				print 'ERROR: uv file %s not updated' %(item[0])
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
	failed_delete = '/data2/home/immwa/scripts/paperdata/failed_deletion.psv'
	delete_files(usrnm, pswd, confirm, failed_delete)
