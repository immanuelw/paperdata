import aipy as A
import sys
import os

### Script to move and rename .uv files with unknown names
### Moves .uv files without names into new directories, creating names fro mreadign through file

### Author: Immanuel Washington
### Date: 8-20-14

#beginning of filename
beg = 'zen'
dot = '.'

#location of directory searched
data = '/mnt/USB/0/D40P01/Good/FOUND.ODR/Orphan9/'

#location of directory to move to
datashift = '/data4/paper/file_renaming_test_output'

#loop over files/folders to look through
for root, dirs, files in os.walk(data):
	for dir in dirs:
		#print dir
		file = os.path.join(root, dir)
		uv = A.miriad.UV(file)
		print file
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
		#copy data from one file to the other
		os.system('cp -r %s/*.uv %s' %(file, newfile))
