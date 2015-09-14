#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to pyganglia

import sys
import os
import paramiko
import dbi as pyg
import psutil
import time

### Script to add info to pyganglia database
### Adds information using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def two_round(num):
	return round(float(num), 2)

def filesystem(ssh, host, path):
	timestamp = int(time.time())
	system_data = {}
	system_data['host'] = host
	system_data['system'] = path
	system_data['timestamp'] = timestamp
	if ssh is None:
		fi = psutil.disk_usage(path)
		total = fi.total
		used = fi.used
		free = fi.free
		percent = fi.percent
	else:
		stdin, folio, stderr = ssh.exec_command('df -B 1')
		for output in folio.split('\n'):
			filesystem = output.split(' ')[-1]
			if filesystem in (path,)
				total = int(output.split(' ')[-4])
				used = int(output.split(' ')[-3])
				free = int(output.split(' ')[-2])
				percent = int(output.split(' ')[-1].split('%')[-1])

	system_data['total_space'] = total
	system_data['used_space'] = used
	system_data['free_space'] = free
	system_data['percent_space'] = percent

	return system_data

def iostat(ssh, host):
	timestamp = int(time.time())
	iostat_data = {}
	if ssh is None:
		io = psutil.disk_io_counters(perdisk=True)
		for device, value in io.items():
			iostat_data[device] = {'host': host}
			iostat_data[device]['timestamp'] = timestamp
			tps = None
			read_s = round(value.read_count / float(value.read_time), 2)
			write_s = round(value.write_count / float(value.write_time), 2)
			bl_reads = value.read_bytes
			bl_writes = value.write_bytes

			iostat_data[device]['device'] = device
			iostat_data[device]['tps'] = tps
			iostat_data[device]['read_s'] = read_s
			iostat_data[device]['write_s'] = write_s
			iostat_data[device]['bl_reads'] = bl_reads
			iostat_data[device]['bl_writes'] = bl_writes

		return iostat_data
	else:
		stdin, folio, stderr = ssh.exec_command('iostat')

		folio_use = []
		folio_name = folio.split('\n')[0].split(' ')[2].strip('()')
		devices = ('sda', 'sda1', 'sda2','dm-0', 'dm-1')
		for output in folio.split('\n'):
			device = output.split(' ')[0]
			if device in devices:
				line = output[:].split(' ')
				new_line = filter(lambda a: a not in [''], line)
				folio_use.append(new_line)

		#Convert all numbers to floats, keep words as strings
		for row in folio_use:
			device = row[0]
			tps = two_round(row[1])
			read_s = two_round(row[2])
			write_s = two_round(row[3])
			bl_reads = int(row[4])
			bl_writes = int(row[5])

			iostat_data[device]['device'] = device
			iostat_data[device]['tps'] = tps
			iostat_data[device]['read_s'] = read_s
			iostat_data[device]['write_s'] = write_s
			iostat_data[device]['bl_reads'] = bl_reads
			iostat_data[device]['bl_writes'] = bl_writes

	return iostat_data

def ram_free(ssh, host):
	#Calculates ram usage on folio
	timestamp = int(time.time())
	ram_data = {}
	ram_data['host'] = host
	ram_data['timestamp'] = timestamp
	if ssh is None:
		ram1 = psutil.virtual_memory()
		ram2 = psutil.swap_memory()
		total = ram1.total
		used = ram1.used	
		free = ram1.available
		shared = ram1.shared
		buffers = ram1.buffers
		cached = ram1.cached
		bc_used = ram1.used - (ram1.buffers + ram1.cached)
		bc_free = ram1.total - bc.used
		swap_total = ram2.total
		swap_used = ram2.used
		swap_free = ram2.free

	else:
		stdin, folio, stderr = ssh.exec_command('free -b')
		ram = []
		for output in folio.split('\n'):
			line = output[:].split(' ')
			new_line = filter(lambda a: a not in [''], line)
			ram.append(new_line)	

		reram = []
		for key, row in enumerate(ram[1:-1]):
			if key == 0:
				total = int(row[1])
				used = int(row[2])
				free = int(row[3])
				shared = int(row[4])
				buffers = int(row[5])
				cached = int(row[6])
			elif key == 1:
				bc_used = int(row[2])
				bc_free = int(row[3])
			elif key == 2:
				swap_total = int(row[1])
				swap_used = int(row[2])
				swap_free = int(row[3])

	ram_data['total'] = total
	ram_data['used'] = used
	ram_data['free'] = free
	ram_data['shared'] = shared
	ram_data['buffers'] = buffers
	ram_data['cached'] = cached
	ram_data['bc_used'] = bc_used
	ram_data['bc_free'] = bc_free
	ram_data['swap_total'] = swap_total
	ram_data['swap_used'] = swap_used
	ram_data['swap_free'] = swap_free

	return ram_data

def cpu_perc(ssh, host):
	#Calculates cpu usage on folio
	timestamp = int(time.time())
	cpu_data = {}
	if ssh is None:
		cpu_all = psutil.cpu_times_percent(interval=1, percpu=True)
		for key, value in enumerate(cpu_all):
			cpu_data[key] = {'host': host}
			cpu_data[key]['timestamp'] = timestamp
			cpu = key
			user_perc = value.user
			sys_perc = value.system
			iowait_perc = value.iowait
			idle_perc = value.idle
			intr_s = None

			cpu_data[key]['cpu'] = key
			cpu_data[key]['user_perc'] = user_perc
			cpu_data[key]['sys_perc'] = sys_perc
			cpu_data[key]['iowait_perc'] = iowait_perc
			cpu_data[key]['idle_perc'] = idle_perc
			cpu_data[key]['intr_s'] = intr_s
		return cpu_data

	else:
		stdin, folio, stderr = ssh.exec_command('mpstat -P ALL 1 1')
		for output in folio.split('\n'):
			if output in (folio.split('\n')[0], folio.split('\n')[1]):
				continue
			line = output[:].split(' ')
			new_line = filter(lambda a: a not in [''], line)
			if new_line[0] not in ['Average:']:
				cpu.append(new_line)
		for key, row in enumerate(cpu[3:]):
			#skip first three lines
			cpu_data[key] = {'host': host}
			cpu_data[key]['timestamp'] = timestamp
			cpu = int(row[2])
			user_perc = two_round(row[3])
			sys_perc = two_round(row[5])
			iowait_perc = two_round(row[6])
			idle_perc = two_round(row[10])
			intr_s = two_round(row[11])

			cpu_data[key]['cpu'] = key
			cpu_data[key]['user_perc'] = user_perc
			cpu_data[key]['sys_perc'] = sys_perc
			cpu_data[key]['iowait_perc'] = iowait_perc
			cpu_data[key]['idle_perc'] = idle_perc
			cpu_data[key]['intr_s'] = intr_s

	return cpu_data

def add_data(ssh, host):
	ssh = pyg.login_ssh(host)
	dbi = pyg.DataBaseInterface()

	iostat_all_data = iostat(ssh, host)
	for name, iostat_data in iostat_all_data.items():
		dbi.add_to_table('iostat', iostat_data)

	ram_data = ram_free(ssh, host)
	dbi.add_to_table('ram', ram_data)

	cpu_all_data = cpu_perc(ssh, host)
	for key, cpu_data in cpu_all_data.items():
		dbi.add_to_table('cpu', cpu_data)

	if host in ('folio',):
		paths = ('/data3', '/data4')
		for path in paths:
			system_data = filesystem(ssh, path)
			dbi.add_to_table('filesystem', system_data)

	ssh.close()

	return None

if __name__ == '__main__':
	hosts = ('folio', 'node01', 'node02', 'node03', 'node04', 'node05', 'node06', 'node07', 'node08', 'node09', 'node10')
	named_host = socket.gethostname()
	for host in hosts:
		if host == named_host:
			add_data(None, host)
		add_data(host)
