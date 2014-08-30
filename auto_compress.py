#!/usr/bin/python
import xml.dom.minidom
import os
import sys
import string
import MySQLdb

### Script to Automate data compression
### Empties paperdistiller, adds in new files, runs scheduler

### Author: Immanuel Washington
### Date: 8-20-14

usrnm = raw_input('Root User: ')
pswd = getpass.getpass('Root Password: ')

nodes = int(raw_input('Amount of nodes: '))

# open a database connection
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('set foreign_key_checks = 0; TRUNCATE neighbors; TRUNCATE observation; TRUNCATE file; set foreign_key_checks = 1')

#close, save and end connection
cursor.close()
connection.commit()
connection.close(

#os.popen('qsub -t 1:%d /usr/global/paper/capo/Compress/ddr_compress/scripts/launch_shredder_compress.sh' %(nodes))

usrm = raw_input('User: ')

f = os.popen('qstat -u \* -xml -r')

dom = xml.dom.minidom.parse(f)


jobs = dom.getElementsByTagName('job_info')
run = jobs[0]

runjobs = run.getElementsByTagName('job_list')

node_number = []

def fakeqstat(joblist):
        for r in joblist:
		#checks if wanted user		
		jobown = r.getElementsByTagName('JB_owner')[0].childNodes[0].data
		if jobown != usrm:
			continue
		#checks if compression script
		jobname = r.getElementsByTagName('JB_name')[0].childNodes[0].data
		if jobname == '': #launch_shredder.sh
			continue #do stuff
		else:
			continue
		#checks if assigned to node and adds it to list
		try:
			jobnode = r.getElementsByTagName('queue_name')[0].childNodes[0].data
		except:
			jobnode = 'NONE'

		node_number.append(jobnode[-6:])

	return node_number

nodenum = fakeqstat(runjobs)

#use dummy variable and list to set up taskserver string
fakenum = 0
task_string = ''
for item in nodenum:
	if fakenum % 2= 0:
		port_num = 14204
	else:
		port_num = 14205
	fakenum += 1

	if fakenum < 4:
		task_string += '%s:%d,' %(item,port_num)
	else:
		task_string += '%s:%d' %(item,port_num)

#os.popen('add_observations.py /data4/paper/still_raw_data_test/psa6678/*.uv')
#os.popen('qmaster_scheduler.py' --taskservers='%s' %(task_string))
#os.popen('monitor_still.py')
