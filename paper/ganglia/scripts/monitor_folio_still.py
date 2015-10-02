#!/usr/global/paper/bin/python
from __future__ import print_function
import ddr_compress.dbi as ddbi
from sqlalchemy import func
import curses,time,os
from paper.ganglia import dbi as pyg

#setup my output file
file_log = []
file_status = {}
file_time = {}
file_pid = {}
file_start = {}
file_end = {}

#setup my curses stuff following
# https://docs.python.org/2/howto/curses.html
stdscr = curses.initscr()
curses.noecho()
curses.cbreak()
stdscr.keypad(1)
stdscr.nodelay(1)

#setup my db connection
dbi = ddbi.DataBaseInterface()
pyg_dbi = pyg.DataBaseInterface()

stdscr.addstr('PAPER Distiller Status Board')
stdscr.addstr(1,0,'Press "q" to exit')
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
	table = getattr(ddbi, 'Observation')
	while(1):
		timestamp = int(time.time())
		log_info = []
		#get the screen dimensions

		#load the currently executing files
		i += 1
		curline = 2
		stdscr.addstr(0,30,stat[i%len(stat)])
		s = dbi.Session()
		totalobs = s.query(table).count()
		stdscr.addstr(curline, 0, 'Number of observations currently in the database: {totalobs}'.format(totalobs=totalobs))
		curline += 1
		OBSs = s.query(table).filter(getattr(table, 'status') != 'NEW').filter(getattr(table, 'status') != 'COMPLETE').all()
		#OBSs = s.query(table).all()
		obsnums = [OBS.obsnum for OBS in OBSs]
		stdscr.addstr(curline, 0, 'Number of observations currently being processed {num}'.format(num=len(obsnums)))
		curline += 1
		statusscr.erase()
		statusscr.addstr(0, 0 ,'  ----  Still Idle  ----   ')
		for j, obsnum in enumerate(obsnums):
			try:
				host, path, filename= dbi.get_input_file(obsnum)
				status = dbi.get_obs_status(obsnum)
				still_host = dbi.get_obs_still_host(obsnum)
				current_pid = dbi.get_obs_pid(obsnum)
			except:
				host, path, filename = 'host', '/path/to/', 'zen.2345672.23245.uv'
				status = 'WTF'
			col = int(j/statusscr.getmaxyx()[0])
			#print(col*colwidth)
			if j == 0 or col == 0:
				row = j
			else:
				row = j % statheight
			try:
				statusscr.addstr(row, col * colwidth,
					'{filename} {status} {still_host}'.format(col=col, filename=os.path.basename(filename),
																status=status, still_host=still_host))
			except:
				continue
			#check for new filenames
			path = os.path.dirname(filename)
			file_name = os.path.basename(filename)
			full_path = ':'.join(still_host, filename)
			if filename not in file_pid.keys():
				file_pid.update({filename:current_pid})
				time_start = int(time.time())
				file_start.update({filename:time_start})
				file_end.update({filename:-1})
			if file_pid[filename] not in [current_pid]:
				time_end = int(time.time())
				file_end.update({filename:time_end})
				del_time = -1
				full_stats = ''.join((full_path, status))
				entry_dict = {'host':still_host,
							'path':path,
							'filename':file_name,
							'full_path':full_path,
							'status':status,
							'full_stats':full_stats,
							'del_time':del_time,
							'time_start':file_start[filename],
							'time_end':file_end[filename],
							'timestamp':timestamp}
				file_log.append(entry_dict)
				file_pid.update({filename:current_pid})
				time_start = int(time.time())
				file_start.update({filename:time_start})
				file_end.update({filename:-1})
			if filename not in file_status.keys():
				file_status.update({filename:status})
				del_time = 0
				full_stats = ''.join((full_path, status))
				entry_dict = {'host':still_host,
							'path':path,
							'filename':file_name,
							'full_path':full_path,
							'status':status,
							'full_stats':full_stats,
							'del_time':del_time,
							'time_start':file_start[filename],
							'time_end':file_end[filename],
							'timestamp':timestamp}
				file_log.append(entry_dict)
				file_time.update({filename:time.time()})
			#write output log
			if file_status[filename] not in [status]:
				del_time = time.time() - file_time[filename]
				full_stats = ''.join((full_path, status))
				entry_dict = {'host':still_host,
							'path':path,
							'filename':file_name,
							'full_path':full_path,
							'status':status,
							'full_stats':full_stats,
							'del_time':del_time,
							'time_start':file_start[filename],
							'time_end':file_end[filename],
							'timestamp':timestamp}
				file_log.append(entry_dict)
				file_status.update({filename:status})
				file_time.update({filename:time.time()})
		with pyg_dbi.session_scope as s:
			for monitor_data in file_log:
				pyg_dbi.add_to_table(s, 'monitor', monitor_data)
		file_log = []
		s.close()
		statusscr.refresh()
		c = stdscr.getch()
		if c == ord('q'):
			break
		time.sleep(1)
except(KeyboardInterrupt):
	s.close()
	pass
#terminate
curses.nocbreak(); stdscr.keypad(0); curses.echo()
curses.endwin()
