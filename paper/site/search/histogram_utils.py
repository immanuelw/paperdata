'''
paper.site.search.histogram_utils

author | Immanuel Washington

Functions
---------
get_plot_bands | gets list of plot bands for a set
get_observation_counts | gets counts of observations
get_file_counts | gets counts of files
get_obs_file_histogram | shows histogram of observations and files
'''
from paper.site import db_utils, misc_utils
from flask import g, render_template
import math
import json

def get_plot_bands(the_set):
	'''
	get plot bands for a set

	Parameters
	----------
	the_set | object: set object

	Returns
	-------
	dict: start, end, and color of flagged subset of set object
	'''
	flagged_subsets = db_utils.query(database='search', table='FlaggedSubset', field_tuples=(('set_id', '==', the_set.id),))

	plot_bands = [{'from': int(flagged_subset.start), 'to': int(flagged_subset.end), 'color': 'yellow'} for flagged_subset in flagged_subsets]

	return plot_bands

def get_observation_counts(start_utc, end_utc, set_pol, set_era_type):
	'''
	get all observations to plot

	Parameters
	----------
	start_utc | int: start time in utc
	end_utc | int: end time in utc
	set_pol | str: set polarization
	set_era_type | str: set era type

	Returns
	-------
	tuple:
		dict: 2D dict of count by polarization and era type
		dict: 2D dict of lists of dicts of time and obsnum by polarization and era type
	'''
	response = db_utils.query(database='paperdata', table='Observation',
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
								('polarization', None if set_pol == 'all' else '==', set_pol),
								('era_type', None if set_era_type == 'all' else '==', set_era_type)),
								sort_tuples=(('time_start', 'asc'),))

	pol_strs, era_type_strs, _, _ = misc_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_type_str: 0 for era_type_str in era_type_strs} for pol_str in pol_strs}

	for obs in response:
		obs_map[obs.polarization][obs.era_type].append({'obs_time': obs.time_start, 'obsnum': obs.obsnum})
		obs_count[obs.polarization][obs.era_type] += 1

	return obs_count, obs_map

def get_file_counts(start_utc, end_utc, host_strs=None, filetype_strs=None, set_host=None, set_filetype=None):
	'''
	get all files to plot

	Parameters
	----------
	start_utc | int: start time in utc
	end_utc | int: end time in utc
	host_strs | Optional[list]: hosts --defaults to None
	filetype_strs | Optional[list]: filetypes --defaults to None
	set_host | Optional[str]: set host --defaults to None
	set_filetype | Optional[str]: set filetype --defaults to None

	Returns
	-------
	tuple:
		dict: 2D dict of count by host and filetype
		dict: 2D dict of lists of dicts of time and obsnum by host and filetype
	'''
	base_length = 0.00696
	try:
		rtp_obs_list = db_utils.query(database='paperdata', table='RTPObservation',
										field_tuples=(('julian_date', '>=', start_utc), ('julian_date', '<=', end_utc + base_length)))

		paper_obs_list = db_utils.query(database='paperdata', table='Observation',
										field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)))

		all_obs_list = rtp_obs_list + paper_obs_list
		files_list = (obs.files for obs in all_obs_list)
		response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)
	except:
		response = (None,)

	if (host_strs is None) or (filetype_strs is None):
		_, _, host_strs, filetype_strs = misc_utils.get_set_strings()

	file_map = {host_str: {filetype_str: [] for filetype_str in filetype_strs} for host_str in host_strs}
	file_count = {host_str: {filetype_str: 0 for filetype_str in filetype_strs} for host_str in host_strs}

	for file_obj in response:
		if file_obj is not None:
			if (set_host and set_host != host) and (set_filetype and set_filetype != filetype):
				continue

			file_map[file_obj.host][file_obj.filetype].append({'file_date': file_obj.julian_date, 'obsnum': file_obj.obsnum})
			file_count[file_obj.host][file_obj.filetype] += 1

	return file_count, file_map

def get_obs_file_histogram(start_utc, end_utc, start_time_str, end_time_str):
	'''
	generate histogram after limiting time

	Parameters
	----------
	start_utc | int: start time in utc
	end_utc | int: end time in utc
	start_time_str | str: start time string
	end_time_str | str: end time string

	Returns
	-------
	html: histogram
	'''
	try:
		response = db_utils.query(database='paperdata', table='Observation',
									field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
									sort_tuples=(('time_start', 'asc'),))
	except:
		response = (None,)

	pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_type_str: 0 for era_type_str in era_type_strs} for pol_str in pol_strs}

	for obs in response:
		if obs is not None:
			obs_map[obs.polarization][obs.era_type].append({'obs_date': obs.julian_date, 'obsnum': obs.obsnum})
			obs_count[obs.polarization][obs.era_type] += 1

	file_count, file_map = get_file_counts(start_utc, end_utc, host_strs, filetype_strs)

	file_map = json.dumps(file_map)
	file_count = json.dumps(file_count)
	obs_map = json.dumps(obs_map)
	obs_count = json.dumps(obs_count)

	#obs_dict = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}
	#file_dict = {host_str: {filetype_str: [] for filetype_str in filetype_strs} for host_str in host_strs}

	return render_template('histogram.html', pol_strs=list(pol_strs), era_type_strs=list(era_type_strs),
							host_strs=list(host_strs), filetype_strs=list(filetype_strs),
							file_map=file_map, file_counts=file_count, obs_map=obs_map, obs_counts=obs_count,
							file_dict=file_map, obs_dict=obs_map,
							range_start=start_time_str, range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
	#return render_template('histogram.html', pol_strs=pol_strs, era_type_strs=era_type_strs, host_strs=host_strs, filetype_strs=filetype_strs,
	#						file_map=file_map, file_count=file_count, obs_map=obs_map, obs_counts=obs_count,
	#						range_start=start_time_str, range_end=end_time_str,
	#						start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
