import aipy as A
import sys
import os
import shutil
import csv
import glob
import decimal

### Script to move and rename .uv files with unknown names
### Moves .uv files without names into new directories, creating names fro mreadign through file

### Author: Immanuel Washington
### Date: 8-20-14

#location of directory searched
data = '/data4/paper/file_renaming_test/*'

#location of directory to move to
datashift = '/data4/paper/file_renaming_test_output/'

#indicates size of directory or file
def get_size(start_path):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
                for f in filenames:
                        fp = os.path.join(dirpath, f)
                        total_size += os.path.getsize(fp)
        return total_size

#dummy count variable
count = 0

#loop over files/folders to look through
dirs = glob.glob(data)
for dir in dirs:
	#create csv file to log bad files
	error_file = open('/data4/paper/file_renaming_test_output/128error.csv', 'a')
	ewr = csv.writer(error_file, dialect='excel')

	count += 1

	#print dir
	data_file = dir

	#check if file size is over 500MB, if not: skip
	if os.path.getsize(data_file) < 524288000:
		continue

	if not os.path.isdir('/data4/paper/file_renaming_test_output/%d.uv' %(count)):
		try:
			os.makedirs('/data4/paper/file_renaming_test_output/%d.uv' %(count))
		except:
			print 'Error creating new directory for %s' %(dir)
			continue
		try:
			for item in glob.glob('/data4/paper/test_rename/empty/*'):
				shutil.copy(item, '/data4/paper/file_renaming_test_output/%d.uv' %(count))
		except:
			print 'Error copying filler data with %s' %(dir)
			continue

	if not os.path.isdir('/data4/paper/file_renaming_test_output/%d.uv' %(count)):
		print 'No directory'
		continue

	#if over 100MB, copy over to folio/copy to new folder and rename
	try:
		shutil.copy(data_file, '/data4/paper/file_renaming_test_output/%d.uv/visdata' %(count))
	except:
		print 'Directory /data4/paper/file_renaming_test_output/%d.uv/ doesnt exist' %(count)
		continue

	#set string to location of new .uv file
	newUV = '/data4/paper/file_renaming_test_output/%d.uv' %(count)

	#allows uv access
	try:
		print 'Accessing uv...'
		#Fixes random fatal error
		uv = 0
		uv = A.miriad.UV(newUV)
		print 'uv Success'
	except:
		item = [newUV, dir,'Cannot access .uv file']
		ewr.writerow(item)
		error_file.close()
		print 'UV Error'
		continue

	print 'jd...'
	#find Julian Date
	try:
		jdate = str(round(uv['time'], 5))
	except:
		print 'JDate error'
		continue

	print 'pol...'
	#assign letters to each polarization
	try:
		if uv['npol'] == 1:
			if uv['pol'] == -5:
				pol = 'xx'
			elif uv['pol'] == -6:
				pol = 'yy'
			elif uv['pol'] == -7:
				pol = 'xy'
			elif uv['pol'] == -8:
				pol = 'yx'

			#create variable to indicate new directory
			newdir = 'zen.' + jdate + '.' + pol + '.uv'
			newfile = os.path.join(datashift, newdir)	

		#if polarizations aren't separated
		if uv['npol'] == 4:
			newdir = 'zen.' + jdate + '.uv'
			newfile = os.path.join(datashift, newdir)

	except:
		print 'Polarization Error'
		continue

	print newfile
		
	#copy data from one file to the other directory
	try:
		shutil.move(newUV,newfile)
	except:
		item = [newfile, dir, '''Couldn't move file''']
		ewr.writerow(item)
		error_file.close()
