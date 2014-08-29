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
#data = '/mnt/USB/0/D40P01/Good/FOUND.ODR/Orphan9/'
data = '/data4/paper/file_renaming_test_output/'

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

#loop over files/folders to look through
for root, dirs, files in os.walk(data):
	for dir in dirs:
		for file in files:
			#print dir
			dummy = os.path.join(root,dir)
			data_file = os.path.join(dummy,file)

			#check if file size is over 100MB, if not: skip
			if get_size(data_file) < 104857600:
				continue
			#allows uv access
			try:
				uv = A.miriad.UV(data_file)
			except:
				error_list = [[data_file,'Cannot access .uv file']]
				for item in error_list:
					ewr.writerow(item)
					continue

			print data_file
			#find Julian Date
			jdate = str(uv['time'])

			#assign letters to each polarization
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
			print newfile
		
			#copy data from one file to the other directory
			shutil.copy(data_file,newfile)
