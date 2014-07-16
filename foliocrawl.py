import os
import inspect
import csv
import aipy as A

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

db = raw_input('32 or 64?: ')

data32 = '/data4/raw_data/'
data64 = '/data4/paper/2012EoR/psa_live/'

db32 = '/data2/home/immwa/scripts/db2_output32.csv'
db64 = '/data2/home/immwa/scripts/db2_output64.csv'


#searches for only particular files
if db == '32':
	datanum = data32
	dbnum = db32
elif db == '64':
	datanum = data64
	dbnum = db64

resultFile = open(dbnum,'wb')



#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#create variable to count files
id = 0
#iterates through directories, listing information about each one
for root, dirs, files in os.walk(datanum):
	#newname indicates directory right before filename
	if db =='32':
		datatruth = len(root) > 26 and len(root) < 34 and root[16] =='p'
	elif db == '64':
		datatruth = len(root) > 36 and len(root) < 64 and root[30] == 'p'
	if datatruth:
		for dir in dirs:
			#if filename ends with uvcRRE, record into file
			if dir[-6:] == 'uvcRRE' and len(dir) > 6:
				#indicates name of full directory
				#rt = root[0:24]

				location = os.path.join(root, dir)

				#indicates size of file
				sz = sizeof_fmt(get_size(location))

				visdata = os.path.join(location, 'visdata')
				if not os.path.isfile(visdata):
					continue

                                #allows uv access
				try:
	                               uv = A.miriad.UV(location)
				except:
					continue	

				#indicates julian date
				jdate = uv['time']


				#indicates julian day
				if datanum == data32:
					jday = location[-28:-25]
				elif datanum == data64:
					jday = location[-29:-25]

				#indicates set of data used
				if datanum == data32:
					antennas = 32
				elif datanum == data64:
					antennas = 64

				#indicates name of file to be used
				filename = dir

				#gives each file unique id FIX WITH OBSNUM!
				id += 1
				#assign letters to each polarization
				if uv['npol'] == 1:
					if uv['pol'] == -5:
						polarization = 'xx'
					elif uv['pol'] == -6:
						polarization = 'yy'
					elif uv['pol'] == -7:
						polarization = 'xy'
					elif uv['pol'] == -8:
						polarization = 'yx' 
				elif uv['npol'] == 4:
					polarization = 'all'

				#indicates length of information in file
				length = uv['inttime'] 

				#location of calibrate files
                                if datanum == data32:
					cal_location = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
                                elif datanum == data64:
					cal_location = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'

				#create list of important data and open csv file
				datab = [[filename,location,antennas,id,jday,jdate,polarization,length,cal_location,str(sz)]]
			#	datab = [[filename,location,antennas,id,jday,jdate,cal_location,str(sz)]]
				print datab 

				#write to csv file by item in list
				for item in datab:
					wr.writerow(item)
