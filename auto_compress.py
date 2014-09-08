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

#function to generate list of nodes used by qsub
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

#login info for paperdistiller database
usrnm = raw_input('Root User: ')
pswd = getpass.getpass('Root Password: ')

# open a database connection
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('set foreign_key_checks = 0; TRUNCATE neighbors; TRUNCATE observation; TRUNCATE file; set foreign_key_checks = 1')

#close, save and end connection
cursor.close()
connection.commit()
connection.close(

#checks which version of compression to be used
version_still = raw_input('Use still.cfg? (y/n): ')

#uses code for each version
if version_still == 'y':
	#rewrite still.cfg 

	#empty list to add lines to
	still_list = []


elif version_still == 'n':
	#indicate amount of nodes used in compression
	nodes = int(raw_input('Amount of nodes: '))

	#set amount of nodes used for compression scripts
	os.popen('qsub -t 1:%d /usr/global/paper/capo/Compress/ddr_compress/scripts/launch_shredder_compress.sh' %(nodes))

	#User to search for jobs
	usrm = raw_input('User of job: ')

	#uses qstat to generate xml, parsed through module
	f = os.popen('qstat -u \* -xml -r')
	dom = xml.dom.minidom.parse(f)
	jobs = dom.getElementsByTagName('job_info')
	run = jobs[0]
	runjobs = run.getElementsByTagName('job_list')

	#empty list to append nodes to
	node_number = []

	#all code above used to generate list of nodes which are used to run the jobs by qsub
	nodenum = fakeqstat(runjobs)

#use dummy variable and list to set up taskserver string
	fakenum = 0
	port_num = 14204
	task_string = ''
	for item in nodenum:
		fakenum += 1
		if fakenum < 4:
			task_string += '%s:%d,' %(item,port_num)
		else:
			task_string += '%s:%d' %(item,port_num)

#create string containing path to .uv files
uv_path = raw_input('Full path of .uv files: ')

#add .uv files to paperdistiller database
os.popen('add_observations.py %s' %(uv_path))

else:
	print 'Wrong input. Use y or n only'
	sys.exit()

#run qmaster scheduler type depending on still.cfg
if version_still == 'y':
	 os.popen('qmaster_scheduler.py')

elif version_still == 'n':
	#run qmaster scheduler to start compressing
	os.popen('qmaster_scheduler.py' --taskservers='%s' %(task_string))
    
#activate script to look at paperdistiller database
os.popen('monitor_still.py')
