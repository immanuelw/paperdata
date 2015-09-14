#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import dbi as pyg
from paperdata.data import dbi as pdbi
from sqlalchemy import func
import sys
import os
import glob
import socket
import time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

### Script to plot the current status of compression nodes
### Reads in data files of each node and outputs relevant plots

### Author: Immanuel Washington
### Date: 05-20-15

def plot_monitor(filenames):
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	host = socket.gethostname()
	STAGEs = ('NEW','UV_POT', 'UV', 'UVC', 'CLEAN_UV', 'UVCR', 'CLEAN_UVC', 'ACQUIRE_NEIGHBORS', 'UVCRE',
				'NPZ', 'UVCRR', 'NPZ_POT', 'CLEAN_UVCRE', 'UVCRRE', 'CLEAN_UVCRR', 'CLEAN_NPZ', 'CLEAN_NEIGHBORS',
				'UVCRRE_POT', 'CLEAN_UVCRRE', 'CLEAN_UVCR', 'COMPLETE')
	x_values = np.arange(1, len(STAGEs) + 1, 1)
	stage_dict = dict(zip(STAGEs, x_values))
	for uv_file in filenames:
		full_path = ':'.join(host, uv_file)
		MONITORs = s.query(pyg.Monitor).filter(pyg.Monitor.full_path==full_path).all()
		file_host = s.query(pyg.Monitor).filter(pyg.Monitor.full_path==full_path).one().host
		status_data = tuple((MONITOR.status, MONITOR.del_time) for MONITOR in MONITORs if MONITOR.del_time is not -1)
		process_data = tuple((MONITOR.status, MONITOR.time_end-MONITOR.time_start) for MONITOR in MONITORs if MONITOR.del_time is -1)
		status_plot = tuple((stage_dict[key], value) for key, value in status_data)
		process_plot = tuple((stage_dict[key], value) for key, value in process_data)

		plt.figure(1)
		plt.subplot(211)
		plt.scatter(*zip(*status_plot), 'b')
		plt.axvline(xdata=x_values, linestyle='--')

		plt.subplot(212)
		plt.scatter(*zip(*process_plot), 'r')
		plt.axvline(xdata=x_values, linestyle='--')

		plt.title(' on '.join(os.path.basename(uv_file), file_host))
		plt.xticks(x_values, STAGEs)
		plt.ylabel('Time Between stages')
		plt.xlabel('Stage')
		plt.grid(True)
		#plt.savefig(os.path.join('/data4/paper/monitor', uv_file.replace('.uv', '.png')))
		plt.show()
	
	s.close()
	return None

def plot_ram(host=None, time_min=None, time_max=None):
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	RAMs = s.query(pyg.Ram)
	if host is not None:
		RAMs = RAMs.filter(pyg.Ram.host==host)
	if time_min is not None:
		RAMs = RAMs.filter(pyg.Ram.timestamp<=time_min)
	if time_max is not None:
		RAMs = RAMs.filter(pyg.Ram.timestamp<=time_max)

	RAMs = RAMs.all()
	s.close()

	total_data = tuple((RAM.timestamp, RAM.total) for RAM in RAMs)
	used_data = tuple((RAM.timestamp, RAM.used) for RAM in RAMs)
	free_data = tuple((RAM.timestamp, RAM.free) for RAM in RAMs)
	shared_data = tuple((RAM.timestamp, RAM.shared) for RAM in RAMs)
	buffers_data = tuple((RAM.timestamp, RAM.buffers) for RAM in RAMs)
	cached_data = tuple((RAM.timestamp, RAM.cached) for RAM in RAMs)
	bc_used_data = tuple((RAM.timestamp, RAM.bc_used) for RAM in RAMs)
	bc_free_data = tuple((RAM.timestamp, RAM.bc_free) for RAM in RAMs)
	swap_total_data = tuple((RAM.timestamp, RAM.swap_total) for RAM in RAMs)
	swap_used_data = tuple((RAM.timestamp, RAM.swap_used) for RAM in RAMs)
	swap_free_data = tuple((RAM.timestamp, RAM.swap_free) for RAM in RAMs)

	plt.figure(1)
	plt.subplot(211)
	plt.plot(*total_data, 'b--')
	plt.plot(*used_data, 'g--')
	plt.plot(*free_data, 'r--')
	plt.plot(*shared_data, 'c--')

	blue_patch = mpatches.Patch(color='blue', label='Total')
	green_patch = mpatches.Patch(color='green', label='Used')
	red_patch = mpatches.Patch(color='red', label='Free')
	cyan_patch = mpatches.Patch(color='cyan', label='Shared')
	plt.legend(handles=[blue_patch, green_patch, red_patch, cyan_patch])

	plt.subplot(212)
	plt.plot(*buffers_data, 'k--')
	plt.plot(*cached_data, 'm--')

	black_patch = mpatches.Patch(color='black', label='Buffered')
	magenta_patch = mpatches.Patch(color='magenta', label='Cached')
	plt.legend(handles=[black_patch, magenta_patch])

	plt.subplot(213)
	plt.plot(*bc_used_data, 'y--')
	plt.plot(*bc_free_data, 'k--')

	yellow_patch = mpatches.Patch(color='yellow', label='BC Used')
	black_patch = mpatches.Patch(color='black', label='BC Free')
	plt.legend(handles=[yellow_patch, black_patch])

	plt.subplot(214)
	plt.plot(*swap_total_data, 'b--')
	plt.plot(*swap_used_data, 'g--')
	plt.plot(*swap_free_data, 'r--')

	blue_patch = mpatches.Patch(color='blue', label='Swap Total')
	green_patch = mpatches.Patch(color='green', label='Swap Used')
	red_patch = mpatches.Patch(color='red', label='Swap Free')
	plt.legend(handles=[blue_patch, green_path, red_patch])

	plt.title('Ram')
	plt.ylabel('Amount')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/ram.png')
	plt.show()

	return None

def plot_iostat(host=None, device=None, time_min=None, time_max=None):
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	IOSTATs = s.query(pyg.Ram)
	if host is not None:
		IOSTATs = IOSTATs.filter(pyg.Iostat.host==host)
	if device is not None:
		IOSTATs = IOSTATs.filter(pyg.Iostat.device==device)
	if time_min is not None:
		IOSTATs = IOSTATs.filter(pyg.Iostat.timestamp<=time_min)
	if time_max is not None:
		IOSTATs = IOSTATs.filter(pyg.Iostat.timestamp<=time_max)

	IOSTATs = IOSTATs.all()
	s.close()

	tps_data = tuple((IOSTAT.timestamp, IOSTAT.tps) for IOSTAT in IOSTATs)
	reads_data = tuple((IOSTAT.timestamp, IOSTAT.read_s) for IOSTAT in IOSTATs)
	writes_data = tuple((IOSTAT.timestamp, IOSTAT.write_s) for IOSTAT in IOSTATs)
	block_read_data = tuple((IOSTAT.timestamp, IOSTAT.bl_reads) for IOSTAT in IOSTATs)
	block_write_data = tuple((IOSTAT.timestamp, IOSTAT.bl_writes) for IOSTAT in IOSTATs)

	plt.figure(1)
	plt.subplot(211)
	plt.plot(*tps_data, 'b--')

	blue_patch = mpatches.Patch(color='blue', label='TPS')
	plt.legend(handles=[blue_patch])

	plt.subplot(212)
	plt.plot(*reads_data, 'g--')
	plt.plot(*writes_data, 'r--')

	green_patch = mpatches.Patch(color='green', label='Reads')
	red_patch = mpatches.Patch(color='red', label='Writes')
	plt.legend(handles=[green_path, red_patch])

	plt.subplot(213)
	plt.plot(*block_read_data, 'k--')
	plt.plot(*block_write_data, 'm--')

	black_patch = mpatches.Patch(color='black', label='Reads per second')
	magenta_patch = mpatches.Patch(color='magenta', label='Writes per second')
	plt.legend(handles=[black_path, magenta_patch])

	plt.title('Iostat')
	plt.ylabel('Amount')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/iostat.png')
	plt.show()

	return None

def plot_cpu(host=None, cpu=None, time_min=None, time_max=None):
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	CPUs = s.query(pyg.Cpu)
	if host is not None:
		CPUs = CPUs.filter(pyg.Cpu.host==host)
	if cpu is not None:
		CPUs = CPUs.filter(pyg.Cpu.cpu==cpu)
	if time_min is not None:
		CPUs = CPUs.filter(pyg.Cpu.timestamp<=time_min)
	if time_max is not None:
		CPUs = CPUs.filter(pyg.Cpu.timestamp<=time_max)

	CPUs = CPUs.all()
	s.close()

	user_perc_data = tuple((CPU.timestamp, CPU.user_perc) for CPU in CPUs)
	sys_perc_data = tuple((CPU.timestamp, CPU.sys_perc) for CPU in CPUs)
	iowait_perc_data = tuple((CPU.timestamp, CPU.iowait_perc) for CPU in CPUs)
	idle_perc_data = tuple((CPU.timestamp, CPU.idle_perc) for CPU in CPUs)
	intr_s_data = tuple((CPU.timestamp, CPU.intr_s) for CPU in CPUs)

	plt.figure(1)
	plt.subplot(211)
	plt.plot(*user_perc_data, 'b--')
	plt.plot(*sys_perc_data, 'g--')
	plt.plot(*iowait_perc_data, 'r--')
	plt.plot(*idle_perc_data, 'k--')

	blue_patch = mpatches.Patch(color='blue', label='User')
	green_patch = mpatches.Patch(color='green', label='System')
	red_patch = mpatches.Patch(color='red', label='I/O wait')
	black_patch = mpatches.Patch(color='black', label='Idle')
	plt.legend(handles=[blue_patch, green_path, red_patch, black_patch])

	plt.subplot(212)
	plt.plot(*intr_s_data, 'm--')

	magenta_patch = mpatches.Patch(color='magenta', label='Instuctions per second')
	plt.legend(handles=[magenta_patch])

	plt.title('Cpu')
	plt.ylabel('Amount')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/cpu.png')
	plt.show()

	return None

def plot_jd_vs_file():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(pyg.Observation.julian_day, func.count(pyg.Observation.julian_day)).group_by(pyg.Observation.julian_day).all()
	s.close()
	jd_data = tuple((OBS[0], OBS[1]) for OBS in OBSs)

	plt.plot(*jd_data, 'r--')

	plt.title('Number of Files in each Julian Day')
	plt.ylabel('Number of Files')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/jd_vs_file.png')
	plt.show()
	
	return None

def plot_jd_vs_gaps():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(pyg.Observation.julian_day, func.count(pyg.Observation.julian_day)).group_by(pyg.Observation.julian_day).all()
	s.close()
	jd_data = tuple((OBS[0], 288 - OBS[1]) if OBS[0] = 128 else (OBS[0], 72 - OBS[1]) for OBS in OBSs)

	plt.plot(*jd_data, 'r--')

	plt.title('Number of Gaps in each Julian Day')
	plt.ylabel('Number of Gaps')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/jd_vs_gaps.png')
	plt.show()
	
	return None

def table_jdate_vs_pol():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(pyg.Observation).all()
	s.close()
	jdate_data = tuple((OBS.julian_date, OBS.polarization) for OBS in OBSs)

	POLs = ('xx', 'xy', 'yx', 'yy', 'all')
	pol_dict = dict(zip(POLs, y_values))
	jdate_plot = tuple((pol_dict[key], value) for key, value in jdate_data)
	y_values = np.arange(1, len(POLs) + 1, 1)

	plt.plot(*jdate_plot, 'r')
	plt.axhline(ydata=y_values-.5, linestyle='--')

	plt.title('Polarization of each Julian Date')
	plt.yticks(y_values, POLs)
	plt.ylabel('Polarization')
	plt.xlabel('Julian Date')
	plt.grid(True)
	#plt.savefig('~/jdate_vs_pol_table.png')
	plt.show()
	
	return None

if __name__ == '__main__':
	if len(sys.argv) > 1:
		filenames = glob.glob(sys.argv[1])
	else:
		filenames = glob.glob(raw_input('Input filename to be plotted: '))
	plot_monitor(filenames)
	#plot_cpu()
	#plot_iostat()
	#plot_ram()
	#plot_jd_vs_file()
	#plot_jd_vs_gaps()
	#table_jdate_vs_pol()
