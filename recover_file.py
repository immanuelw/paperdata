import aipy as A
import sys
import os
import shutil

### Script to move and rename .uv files with unknown names
### Moves .uv files without names into new directories, creating names fro mreadign through file

### Author: Immanuel Washington
### Date: 8-20-14

#location of directory searched
#data = '/mnt/USB/0/D40P01/Good/FOUND.ODR/Orphan9/'
data = '/data4/paper/file_renaming_test/'

#location of directory to move to
datashift = '/data4/paper/file_renaming_test_output/'

#dummy variable
count = 0

#loop over files/folders to look through
for root, dirs, files in os.walk(data):
	for dir in dirs:
		for file in files:
			#create directory to insert unnamed files into
			data_dir = '%s/%s' %(datashift,count)
			#os.mkdir(data_dir)
			count += 1

			#find path of file
			dummy = os.path.join(root,dir)
			data_file = os.path.join(dummy,file)
			
			#copy file to new directory
			shutil.copy(data_file,data_dir)
