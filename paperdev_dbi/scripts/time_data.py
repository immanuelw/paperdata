#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paperdev

import sys
import aipy as A
import os

### Script to calculate uv data on any/other hosts
### output time_data in csv format: 

### Author: Immanuel Washington
### Date: 5-06-15

def calc_time_data(host, full_path):
	filetype = full_path.split('.')[-1]
	#allows uv access
	if filetype in ('uv', 'uvcRRE'):
		try:
			uv = A.miriad.UV(full_path)
		except:
			return None

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

	uv_info = (time_start, time_end, delta_time)

	return uv_info

if __name__ == '__main__':
	input_host = sys.argv[1]
	input_path = sys.argv[2]

	time_data = calc_time_data(input_host, input_path)
	if time_data is None:
		sys.exit()
	output_string = str(time_data[0])
	for data in time_data[1:]:
		output_string += ',' + str(data)
	print output_string
