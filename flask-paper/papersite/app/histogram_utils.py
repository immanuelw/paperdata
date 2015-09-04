from app import db_utils, models
from flask import g, render_template
import math
import json

def get_plot_bands(the_set):
	flagged_subsets = db_utils.query(database='eorlive', table='flagged_subset', field_tuples=(('set_id', '==', getattr(the_set, 'id')),))

	plot_bands = [{'from': int(getattr(flagged_subset, 'start')), 'to': int(getattr(flagged_subset, 'end')), 'color': 'yellow'}
					for flagged_subset in flagged_subsets]

	return plot_bands

def get_observation_counts(start_utc, end_utc, set_pol, set_era_type):
	response = db_utils.query(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
										('polarization', None if set_pol == 'all' else '==', set_pol),
										('era_type', None if set_era_type == 'all' else '==', set_era_type)),
										sort_tuples=(('time_start', 'asc'),),
										output_vars=('time_start', 'time_end', 'polarization', 'era', 'era_type'))

	pol_strs, era_type_strs = db_utils.set_strings()
	obs_map = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_type_str: {'obs_count': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}

	for obs in response:
		polarization = getattr(obs, 'polarization')
		era_type = getattr(obs, 'era_type')

		# Actual UTC time of the obs (for the graph)
		obs_time = getattr(obs, 'time_start')
		obsnum = getattr(obs, 'obsnum')

		obs_map[polarization][era_type].append({'obs_time': obs_time, 'obsnum': obsnum})
		obs_count[polarization][era_type]['obs_count'] += 1

	return (obs_count, obs_map)

def get_file_counts(start_utc, end_utc, host_strs=None, filetype_strs=None, set_host=None, set_filetype=None):
	base_length = 0.00696
	try:
		rtp_obs_list = db_utils.query(database='paperdata', table='rtp_observation',
										field_tuples=(('julian_date', '>=', start_utc), ('julian_date', '<=', end_utc + base_length)),
										output_vars=('obsnum', 'julian_date', 'polarization', 'length', 'files'))

		paper_obs_list = db_utils.query(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
										output_vars=('obsnum', 'julian_date', 'polarization', 'length', 'files'))

		all_obs_list = rtp_obs_list + paper_obs_list
		files_list = (getattr(obs, 'files') for obs in all_obs_list)
		response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)
	except:
		response = (None,)

	if (host_strs is None) or (filetype_strs is None):
		_, _, host_strs, filetype_strs = db_utils.get_set_strings()

	file_map = {host_str: {filetype_str: [] for filetype_str in filetype_strs} for host_str in host_strs}
	file_count = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

	for file_obj in response:
		if not file_obj is None:
			host = getattr(file_obj, 'host')
			filetype = getattr(file_obj, 'filetype')

			if (set_host and set_host != host) and (set_filetype and set_filetype != filetype):
				continue

			file_obs = getattr(file_obj, 'observation')
			obsnum = getattr(file_obs, 'obsnum')
			julian_date = getattr(file_obs, 'julian_date')

			file_map[host][filetype].append({'file_date': julian_date, 'obsnum': obsnum})
			file_count[host][filetype]['file_count'] += 1

	return (file_count, file_map)

def get_obs_file_histogram(start_utc, end_utc, start_time_str, end_time_str):
	try:
		response = db_utils.query(database='paperdata', table='observation',
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
								sort_tuples=(('time_start', 'asc'),),
								output_vars=('julian_date', 'polarization', 'era_type', 'obsnum'))
	except:
		response = (None,)

	pol_strs, era_type_strs, host_strs, filetype_strs = db_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_type_str: {'obs_count': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}

	for obs in response:
		if not obs is None:
			polarization = getattr(obs, 'polarization')
			era_type = getattr(obs, 'era_type')

			obs_date = getattr(obs, 'julian_date')
			obsnum = getattr(obs, 'obsnum')

			obs_map[polarization][era_type].append({'obs_date': obs_date, 'obsnum': obsnum})
			obs_count[polarization][era_type]['obs_count'] += 1

	file_count, file_map = get_file_counts(start_utc, end_utc, host_strs, filetype_strs)

	file_map = json.dumps(file_map)
	file_count = json.dumps(file_count)
	obs_map = json.dumps(obs_map)
	obs_count = json.dumps(obs_count)

	return render_template('histogram.html', pol_strs=pol_strs, era_type_strs=era_type_strs, host_strs=host_strs, filetype_strs=filetype_strs,
							file_map=file_map, file_count=file_count, obs_map=obs_map, obs_count=obs_count,
							range_start=start_time_str, range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
