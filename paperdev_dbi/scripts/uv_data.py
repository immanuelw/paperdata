#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdev

import sys
import aipy as A
import os

### Script to calculate uv data on any/other hosts
### output uv_data in csv format: 

### Author: Immanuel Washington
### Date: 5-06-15

def calc_uv_data(host, full_path):
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

		#gives each file unique id
		if length > 0:
			obsnum = jdpol2obsnum(julian_date, polarization, length)
		else:
			obsnum = None

	uv_info = (round(time_start, 5), round(time_end, 5), round(delta_time, 5), julian_date, polarization, length, obsnum)

	return uv_info

if __name__ == '__main__':
	input_host = sys.argv[1]
	input_path = sys.argv[2]

	uv_data = calc_uv_data(input_host, input_path)
	if uv_data is None:
		sys.exit()
	output_string = str(uv_data[0])
	for data in uv_data[1:]:
		output_string += ',' + str(data)
	print output_string
