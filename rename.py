import aipy as A
import sys
import os
import shutil
import csv

### Script to move and rename .uv files with unknown names
### Moves .uv files without names into new directories, creating names fro mreadign through file

### Author: Immanuel Washington
### Date: 8-20-14

#beginning of filename
beg = 'zen'
dot = '.'

#location of directory searched
data = '/mnt/USB/0/D40P01/Good/FOUND.ODR/Orphan9/'
#data = '/data4/paper/file_renaming_test_output/'

#location of directory to move to
datashift = '/data4/paper/file_renaming_test_output_complete/'

#create csv file to log bad files
error_file = open('/data2/home/immwa/scripts/paper_output/128error.csv', 'a')
ewr = csv.writer(error_file, dialect='excel')

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
for root, dirs, files in os.walk(data):
	for dir in dirs:
		for file in files:
			count += 1
			#print dir
			dummy = os.path.join(root,dir)
			data_file = os.path.join(dummy,file)

			#check if file size is over 100MB, if not: skip
			if get_size(data_file) < 104857600:
				continue

			#if over 100MB, copy over to folio/copy to new folder and rename
			try:
				shutil.copy(data_file, '/data4/paper/file_renaming_test_output/%d/visdata' %(count))
			except:
				print 'Directory /data4/paper/file_renaming_test_output/%d/ doesnt exist' %(count)

			#set string to location of new .uv file
			newUV = '/data4/paper/file_renaming_test_output/%d' %(count)

			#allows uv access
			try:
				uv = A.miriad.UV(newUV)
			except:
				error_list = [[newUV,'Cannot access .uv file']]
				for item in error_list:
					ewr.writerow(item)
				continue

			#find Julian Date
			jdate = str(uv['time'])

			#assign letters to each polarization
			if uv[npol] == 1:
				if uv['pol'] == -5:
					pol = 'xx'
				elif uv['pol'] == -6:
					pol = 'yy'
				elif uv['pol'] == -7:
					pol = 'xy'
				elif uv['pol'] == -8:
					pol = 'yx'

				#create variable to indicate new directory
				newdir = beg + dot + jdate + dot + pol + dot + 'uv'
				newfile = os.path.join(datashift, newdir)	

			#if polarizations aren't separated
			else:
				newdir = beg + dot + jdate + dot + 'uv'
				newfile = os.path.join(datashift, newdir)

			print newfile
		
			#copy data from one file to the other directory
			shutil.move(newUV,newfile)
