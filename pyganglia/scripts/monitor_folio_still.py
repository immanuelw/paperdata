#!/usr/global/paper/bin/python
from ddr_compress.dbi import DataBaseInterface,Observation,File
from sqlalchemy import func
import curses,time,os
import csv
import jdcal
import pyganglia as pyg
import MySQLdb

def write_file(log_info, node_data, title):
	dbr = open(node_data, 'ab')
	wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')
	wr.writerow([title])
	for row in log_info:
		wr.writerow(row)
	dbr.close()
	return None

def write_db(usrnm, pswd, log_info):
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	insert_base = '''INSERT INTO %s VALUES(%s)'''
	for row in log_info:
		val = tuple(row)
		insert = insert_base % (pyg.monitor_files().table, pyg.monitor_files().values)
		values = val
		cursor.execute(insert, values)

	#Close and save changes to database
	cursor.close()
	connection.commit()
	connection.close()

	return None

#user info
usrnm = 'immwa'
pswd = 'immwa3978'

#setup my output file
#node_data = '/data4/paper/paperoutput/monitor_folio_log.psv'
time_d = time.strftime('%d-%m-%Y')
file_data = '/data4/paper/paperoutput/monitor_folio_log_%s.psv' %(time_d)
file_log = []
file_status = {}
file_time = {}

#setup my curses stuff following
# https://docs.python.org/2/howto/curses.html
stdscr = curses.initscr()
curses.noecho()
curses.cbreak()
stdscr.keypad(1)
stdscr.nodelay(1)

#setup my db connection
dbi = DataBaseInterface()

stdscr.addstr("PAPER Distiller Status Board")
stdscr.addstr(1,0,"Press 'q' to exit")
statheight = 50
statusscr = curses.newwin(statheight,200,5,0)
statusscr.keypad(1)
statusscr.nodelay(1)
curline = 2
colwidth = 50
obslines = 20
i=0
stat = ['\\','|','/','-','.']
try:
	while(1):
		time_date = time.strftime('%Y:%m:%d:%H:%M:%S')
		temp_time = time_date.split(':')
		time_date = jdcal.gcal2jd(temp_time[0],temp_time[1],temp_time[2],temp_time[3],temp_time[4],temp_time[5])
		log_info = []
		#get the screen dimensions

		#load the currently executing files
		i += 1
		curline = 2
		stdscr.addstr(0,30,stat[i%len(stat)])
		s = dbi.Session()
		totalobs = s.query(Observation).count()
		stdscr.addstr(curline,0,"Number of observations currently in the database: {totalobs}".format(totalobs=totalobs))
		curline += 1
		OBSs = s.query(Observation).filter(Observation.status!='NEW').filter(Observation.status!='COMPLETE').all()
		#OBSs = s.query(Observation).all()
		obsnums = [OBS.obsnum for OBS in OBSs]
		stdscr.addstr(curline,0,"Number of observations currently being processed {num}".format(num=len(obsnums)))
		curline += 1
		statusscr.erase()
		statusscr.addstr(0,0,"  ----  Still Idle  ----   ")
		for j,obsnum in enumerate(obsnums):
			try:
				host,path,filename= dbi.get_input_file(obsnum)
				status = dbi.get_obs_status(obsnum)
				still_host = dbi.get_obs_still_host(obsnum)
			except:
				host,path,filename = 'host','/path/to/','zen.2345672.23245.uv'
				status = 'WTF'
			col = int(j/statusscr.getmaxyx()[0])
			#print col*colwidth
			if j==0 or col==0:
				row = j
			else:
				row = j%statheight
			try:
				statusscr.addstr(row,col*colwidth,"{filename} {status} {still_host}".format(col=col,filename=os.path.basename(filename),status=status,still_host=still_host))
			except:
				continue
			#check for new filenames
			if filename not in file_status.keys():
				file_status.update({filename:status})
				del_time = 0
				file_log.append((filename,status,del_time,still_host,time_date))
				file_time.update({filename:time.time()})
			#write output log
			if file_status[filename] not in [status]:
				del_time = time.time() - file_time[filename]
				file_log.append((filename,status,del_time,still_host,time_date))
				file_status.update({filename:status})
		write_file(file_log, file_data, '\n')
		write_db(usrnm, pswd, file_log)
		s.close()
		statusscr.refresh()
		c = stdscr.getch()
		if c==ord('q'):
			break
		time.sleep(1)
except(KeyboardInterrupt):
	s.close()
	pass
#terminate
curses.nocbreak(); stdscr.keypad(0); curses.echo()
curses.endwin()
