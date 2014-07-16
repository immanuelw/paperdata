import os
import inspect
import csv

def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	return "%3.1f%s" % (num, 'TB')

resultFile = open('database_output.csv','wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#iterates through directories, listing information about each one
for root, dirs, files in os.walk('/home/yatsutora/test_folio'):
	for newname in dirs:
		#indicates name of directory holding information
		rt = root[-10:]
		#indicates julian date, indices changeable depending on string
		jd = newname[4:17]
		location = os.path.join(root, newname)
                #indicates size of file
		sz = sizeof_fmt(get_size(location))

		#create list of important data and open csv file
		datab = [[location,rt,jd,str(sz)]]
		
		#write to csv file by item in list
		for item in datab:
			wr.writerow(item)
