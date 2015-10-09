#!/usr/global/paper/bin/python
from __future__ import print_function
from sqlalchemy import func
import curses, time, os
from paper.distiller import dbi as ddbi
from paper.ganglia import dbi as pyg

#setup my output file
file_dict = {'status': {}, 'time': {}, 'pid': {}, 'start': {}, 'end': {}}

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
stdscr.addstr(1, 0, 'Press "q" to exit')
statheight = 50
statusscr = curses.newwin(statheight, 200, 5, 0)
statusscr.keypad(1)
statusscr.nodelay(1)
curline = 2
colwidth = 50
obslines = 20
i = 0
stat = ['\\','|','/','-','.']
try:
	table = getattr(ddbi, 'Observation')
	while True:
		timestamp = int(time.time())
		#creates base list
		file_log = []
		#get the screen dimensions

		#load the currently executing files
		i += 1
		curline = 2
		stdscr.addstr(0, 30, stat[i % len(stat)])
		with ddbi.session_scope() as s:
			totalobs = s.query(table).count()
			stdscr.addstr(curline, 0, 'Number of observations currently in the database: {totalobs}'.format(totalobs=totalobs))
			curline += 1
			OBSs = s.query(table).filter(getattr(table, 'status') != 'NEW').filter(getattr(table, 'status') != 'COMPLETE').all()
			#OBSs = s.query(table).all()
			obs_len = len(OBSs)
			stdscr.addstr(curline, 0, 'Number of observations currently being processed {num}'.format(num=obs_len))
			curline += 1
			statusscr.erase()
			statusscr.addstr(0, 0 ,'  ----  Still Idle  ----   ')
			for j, OBS in enumerate(OBSs):
				try:
					obsnum = getattr(OBS, 'obsnum')
					FILE = dbi.get_entry(ddbi, s, 'file', obsnum)
					host = getattr(FILE, 'host')
					base_path, filename = os.path.split(getattr(FILE, 'filename'))
					status = getattr(OBS, 'status')
					still_host = getattr(OBS, 'stillhost')
					current_pid = getattr(OBS, 'currentpid')
				except:
					host, base_path, filename = 'host', '/path/to/', 'zen.2345672.23245.uv'
					status = 'WTF'

				col = int(j / statusscr.getmaxyx()[0])
				#print(col * colwidth)

				if j == 0 or col == 0:
					row = j
				else:
					row = j % statheight

				try:
					statusscr.addstr(row, col * colwidth, ' '.join((filename, status, still_host)))
				except:
					continue

				#check for new filenames
				full_path = ':'.join((still_host, os.path.join(base_path, filename)))
				full_stats = '&'.join((full_path, status))

				if filename not in file_dict['pid'].keys():
					file_dict['pid'].update({filename: current_pid})
					file_dict['start'].update({filename: int(time.time())})
					file_dict['end'].update({filename: -1})

				if file_dict['pid'][filename] != current_pid:
					file_dict['end'].update({filename: int(time.time())})
					entry_dict = {'host': still_host,
									'base_path': base_path,
									'filename': filename,
									'full_path': full_path,
									'status': status,
									'full_stats': full_stats,
									'del_time': -1,
									'time_start': file_dict['start'][filename],
									'time_end': file_dict['end'][filename],
									'timestamp': timestamp}
					file_log.append(entry_dict)
					file_dict['pid'].update({filename: current_pid})
					file_dict['start'].update({filename: int(time.time())})
					file_dict['end'].update({filename: -1})

				if filename not in file_dict['status'].keys():
					file_dict['status'].update({filename: status})
					entry_dict = {'host': still_host,
									'base_path': base_path,
									'filename': filename,
									'full_path': full_path,
									'status': status,
									'full_stats': full_stats,
									'del_time': 0,
									'time_start': file_dict['start'][filename],
									'time_end': file_dict['end'][filename],
									'timestamp': timestamp}
					file_log.append(entry_dict)
					file_dict['time'].update({filename: time.time()})

				#write output log
				if file_dict['status'][filename] != status:
					entry_dict = {'host': still_host,
									'base_path': base_path,
									'filename': file_name,
									'full_path': full_path,
									'status': status,
									'full_stats': full_stats,
									'del_time': int(time.time() - file_dict['time'][filename]),
									'time_start': file_dict['start'][filename],
									'time_end': file_dict['end'][filename],
									'timestamp': timestamp}
					file_log.append(entry_dict)
					file_dict['status'].update({filename: status})
					file_dict['time'].update({filename: time.time()})

			with pyg_dbi.session_scope() as sess:
				for monitor_data in file_log:
					pyg_dbi.add_entry_dict(sess, 'Monitor', monitor_data)

			statusscr.refresh()
			c = stdscr.getch()
			if c == ord('q'):
				break
			time.sleep(1)

except(KeyboardInterrupt):
	pass
#terminate
curses.nocbreak()
stdscr.keypad(0)
curses.echo()
curses.endwin()
