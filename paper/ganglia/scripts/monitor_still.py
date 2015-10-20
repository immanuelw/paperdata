'''
paper.ganglia.scripts.monitor_still

monitors still of paperdistiller database
shows compression progress of uv* files

author | Immanuel Washington
'''
from __future__ import print_function
import os
import copy
import curses
import time
from paper.distiller import dbi as ddbi
from paper.ganglia import dbi as pyg

if __name__ == '__main__':
	#setup my output file
	file_dict = {'status': {}, 'time': {}, 'pid': {}, 'start': {}, 'end': {}}

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
	colwidth = 50
	i = 0
	stat = ['\\','|','/','-','.']
	try:
		table = ddbi.Observation
		while True:
			timestamp = int(time.time())
			#creates base list
			file_log = []
			#get the screen dimensions

			#load the currently executing files
			i += 1
			curline = 2
			stdscr.addstr(0, 30, stat[i % len(stat)])
			with dbi.session_scope() as s:
				totalobs = s.query(table).count()
				stdscr.addstr(curline, 0, 'Number of observations currently in the database: {totalobs}'.format(totalobs=totalobs))
				curline += 1
				OBSs = s.query(table).filter(table.status != 'NEW').filter(table.status != 'COMPLETE').all()
				#OBSs = s.query(table).all()
				stdscr.addstr(curline, 0, 'Number of observations currently being processed {num}'.format(num=len(OBSs))
				curline += 1
				statusscr.erase()
				statusscr.addstr(0, 0 ,'  ----  Still Idle  ----   ')
				for j, OBS in enumerate(OBSs):
					try:
						obsnum = OBS.obsnum
						FILE = dbi.get_entry(ddbi, s, 'file', obsnum)
						host = FILE.host
						base_path, filename = os.path.split(FILE.filename)
						status = OBS.status
						still_host = OBS.stillhost
						current_pid = OBS.currentpid
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
					source = ':'.join((still_host, os.path.join(base_path, filename)))
					full_stats = '&'.join((source, status))
					entry_dict = {'host': still_host,
									'base_path': base_path,
									'filename': filename,
									'source': source,
									'status': status,
									'full_stats': full_stats,
									'del_time': None,
									'time_start': None,
									'time_end': None,
									'timestamp': timestamp}

					if filename not in file_dict['pid'].keys():
						file_dict['pid'].update({filename: current_pid})
						file_dict['start'].update({filename: int(time.time())})
						file_dict['end'].update({filename: -1})

					if file_dict['pid'][filename] != current_pid:
						file_dict['end'].update({filename: int(time.time())})
						pid_entry = copy.deepcopy(entry_dict)
						change_pid = {'del_time': -1,
										'time_start': file_dict['start'][filename],
										'time_end': file_dict['end'][filename]}
						pid_entry.update(change_pid)

						file_log.append(pid_entry)
						file_dict['pid'].update({filename: current_pid})
						file_dict['start'].update({filename: int(time.time())})
						file_dict['end'].update({filename: -1})

					if filename not in file_dict['status'].keys():
						file_dict['status'].update({filename: status})
						status_entry = copy.deepcopy(entry_dict)
						change_status = {'del_time': 0,
										'time_start': file_dict['start'][filename],
										'time_end': file_dict['end'][filename]}
						status_entry.update(change_status)
						file_log.append(status_entry)
						file_dict['time'].update({filename: int(time.time())})

					if file_dict['status'][filename] != status:
						status_entry = copy.deepcopy(entry_dict)
						change_status = {'del_time': int(time.time() - file_dict['time'][filename]),
										'time_start': file_dict['start'][filename],
										'time_end': file_dict['end'][filename]}
						status_entry.update(change_status)
						file_log.append(status_entry)
						file_dict['status'].update({filename: status})
						file_dict['time'].update({filename: int(time.time())})

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
