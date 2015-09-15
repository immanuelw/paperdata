#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdata

from __future__ import print_function
import sys
import os
import dbi as pdbi
import paperdata.convert as convert
import aipy as A

### Script to calculate uv data on any/other hosts
### output uv_data in csv format: 

### Author: Immanuel Washington
### Date: 5-06-15
def five_round(num):
	return round(num, 5)

def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = A.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

def get_lst(julian_date):
	gmst = convert.juliandate_to_gmst(julian_date)
	lst = convert.gmst_to_lst(gmst, longitude=25)
	return round(lst, 1)

def julian_era(julian_date):
	#indicates julian day and set of data
	if julian_date < 2456100:
		era = 32
	elif julian_date < 2456400:
		era = 64
	else:
		era = 128

	julian_day = int(str(julian_date)[3:7])

	return era, julian_day

def is_edge(prev_obs, next_obs):
	if (prev_obs, next_obs) == (None, None):
		edge = None
	else:
		edge = (None in (prev_obs, next_obs))
	return edge

def obs_pn(s, obsnum):
	table = getattr(pdbi, 'Observation')
	PREV_OBS = s.query(table).filter(getattr(table, 'obsnum') == obsnum - 1).one()
	if PREV_OBS is not None:
		prev_obs = getattr(PREV_OBS, 'obsnum')
	else:
		prev_obs = None
	NEXT_OBS = s.query(table).filter(getattr(table, 'obsnum') == obsnum + 1).one()
	if NEXT_OBS is not None:
		next_obs = getattr(NEXT_OBS, 'obsnum')
	else:
		next_obs = None

	return prev_obs, next_obs

def obs_edge(obsnum, sess=None):
	#unknown prev/next observation
	if obsnum == None:
		prev_obs = None
		next_obs = None
	else:
		if sess is None:
			dbi = pdbi.DataBaseInterface()
			s = dbi.Session()
			prev_obs, next_obs = obs_pn(s, obsnum)
			s.close()
		else:
			prev_obs, next_obs = obs_pn(sess, obsnum)

	edge = is_edge(prev_obs, next_obs)

	return prev_obs, next_obs, edge

def calc_times(uv):
	#takes in uv file and calculates time based information
	time_start = 0
	time_end = 0
	n_times = 0
	c_time = 0

	try:
		for (uvw, t, (i,j)),d in uv.all():
			if time_start == 0 or t < time_start:
				time_start = t
			if time_end == 0 or t > time_end:
				time_end = t
			if c_time != t:
				c_time = t
				n_times += 1
	except:
		return None

	if n_times > 1:
		delta_time = -(time_start - time_end)/(n_times - 1)
	else:
		delta_time = -(time_start - time_end)/(n_times)

		length = round(n_times * delta_time, 5)
		time_start = five_round(time_start)
		time_end = five_round(time_end)
		delta_time = five_round(delta_time)

	return time_start, time_end, delta_time, length

def calc_uv_data(host, full_path, mode=None):
	filetype = full_path.split('.')[-1]
	#allows uv access
	if filetype in ('uv', 'uvcRRE'):
		try:
			uv = A.miriad.UV(full_path)
		except:
			return None

		#indicates julian date
		julian_date = round(uv['time'], 5)

		#assign letters to each polarization
		if uv['npol'] == 1:
			polarization = pol_dict[uv['pol']]
		elif uv['npol'] == 4:
			polarization = 'all'

		time_start, time_end, delta_time, length = calc_times(uv)
		if mode == 'time':
			return time_start, time_end, delta_time, length

		#gives each file unique id
		if length > 0:
			obsnum = jdpol2obsnum(julian_date, polarization, length)
		else:
			obsnum = None

		uv_info = (time_start, time_end, delta_time, julian_date, polarization, length, obsnum)
		return uv_info
	else:
		return None

if __name__ == '__main__':
	input_host = sys.argv[1]
	input_path = sys.argv[2]
	if len(sys.argv) == 4:
		mode = sys.argv[3]
	else:
		mode = None

	uv_data = calc_uv_data(input_host, input_path, mode)
	if uv_data is None:
		sys.exit()
	output_string = ','.join(uv_data)
	print(output_string)
