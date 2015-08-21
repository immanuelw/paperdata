from app import db_utils, models
from flask import g, render_template
import math

def get_error_counts(start_gps, end_gps):
	error_counts = []
	error_count = 0

	obscontroller_response = db_utils.query(database='eor', table='obscontroller_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time', 'observation_number', 'comment'))
	obscontroller_response = tuple(math.floor(ref_time) for ref_time in obscontroller_response)

	recvstatuspolice_response = db_utils.query(database='eor', table='recvstatuspolice_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time',))
	recvstatuspolice_response = tuple(math.floor(ref_time) for ref_time in recvstatuspolice_response)

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	prev_time = 0

	for error in obscontroller_response:
		utc_millis = int((error[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)
		if utc_millis == prev_time:
			error_counts[-1][1] += 1
		else:
			error_counts.append([utc_millis, 1])
			prev_time = utc_millis
		error_count += 1

	prev_time = 0

	for error in recvstatuspolice_response:
		utc_millis = int((error[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)
		if utc_millis == prev_time:
			error_counts[-1][1] += 1
		else:
			error_counts.append([utc_millis, 1])
			prev_time = utc_millis
		error_count += 1

	error_counts.sort(key=lambda error: error[0])

	return (error_counts, error_count)

def get_observation_counts(start_gps, end_gps, polarization_var, era_var, era_type_var):
	response = db_utils.query(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_gps), ('time_end', '<=', end_gps),
										('polarization', None if polarization_var == 'any' else '==', polarization_var),
										('era', None if era_var == 0 else '==', era_var),
										('era_type', None if era_type_var == 'any' else '==', era_type_var)),
										sort_tuples=(('time_start', 'asc'),),
										output_vars=('time_start', 'time_end', 'polarization', 'era'. 'era_type'))

	pol_strs, era_strs, era_type_strs = db.utils.set_strings()
	obs_map = {pol_str: {era_str: {era_type: [] for era_type_str in era_type_strs} for era_str in era_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_str: {era_type: {'obs_count': 0} for era_type_str in era_type_strs}
																for era_str in era_strs} for pol_str in pol_strs}

	for obs in response:
		polarization = getattr(obs, 'polarization')
		era = getattr(obs, 'era')
		era_type = getattr(obs, 'era_type')

		# Actual UTC time of the obs (for the graph)
		obs_time = getattr(obs, 'time_start')
		obsnum = getattr(obs, 'obsnum')

		obs_map[polarization][era][era_type].append({'obs_time':obs_time, 'obsnum':obsnum})
		obs_count[polarization][era][era_type]['obs_count'] += 1

	return (obs_count, obs_map)

def get_plot_bands(the_set):
	flagged_subsets = db_utils.query(database='eorlive', table='flagged_subset',
													field_tuples=(('set_id', '==', getattr(the_set, 'id')),))

	plot_bands = [{'from': int((getattr(flagged_subset, 'start'), 'to': int((getattr(flagged_subset, 'end'), 'color': 'yellow'}
					for flagged_subset in flagged_subsets]

	return plot_bands

def get_obs_err_histogram(start_gps, end_gps, start_time_str, end_time_str):
	response = db_utils.query(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_gps), ('time_end', '<=', end_gps),
										sort_tuples=(('time_start', 'asc'),),
										output_vars=('time_start', 'polarization', 'era', 'era_type', 'obsnum')))

	pol_strs, era_strs, era_type_strs = db.utils.set_strings()
	obs_map = {pol_str: {era_str: {era_type: [] for era_type_str in era_type_strs} for era_str in era_strs} for pol_str in pol_strs}
	obs_count = {pol_str: {era_str: {era_type: {'obs_count': 0} for era_type_str in era_type_strs}
																for era_str in era_strs} for pol_str in pol_strs}

	for obs in response:
		polarization = getattr(obs, 'polarization')
		era = getattr(obs, 'era')
		era_type = getattr(obs, 'era_type')

		# Actual UTC time of the obs (for the graph)
		obs_time = getattr(obs, 'time_start')
		obsnum = getattr(obs, 'obsnum')

		obs_map[polarization][era][era_type].append({'obs_time':obs_time, 'obsnum':obsnum})
		obs_count[polarization][era][era_type]['obs_count'] += 1

	error_counts, error_count = get_error_counts(start_gps, end_gps)

	return render_template('histogram.html', error_counts=error_counts, pol_strs=pol_strs, era_strs=era_strs, era_type_strs=era_type_strs,
							obs_count=obs_count, obs_map=obs_map, range_start=start_time_str, range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])

def get_file_histogram(start_gps, end_gps, start_time_str, end_time_str):
	sort_tuples = (('timestamp', 'desc'),)
	output_vars = ('timestamp', 'julian_day')

	rtp_obs_list = db_utils.query(database='paperdata', table='rtp_observation',
												field_tuples=(('julian_date', '>=', start_gps), ('julian_date', '<=', end_gps)),
												output_vars=('obsnum', 'julian_day', 'polarization', 'era' 'length', 'files'))

	paper_obs_list = db_utils.query(database='paperdata', table='observation',
												field_tuples=(('time_start', '>=', start_gps), ('time_end', '<=', end_gps)),
												output_vars=('obsnum', 'julian_day', 'polarization', 'era' 'length', 'files'))

	all_obs_list = rtp_obs_list + paper_obs_list

	obs_dict = {getattr(obs, 'obsnum'): {'julian_day': getattr(obs, 'julian_day'), 'polarization': getattr(obs, 'polarization'),
											'era': getattr(obs, 'era'), 'length': getattr(obs, 'length')} for obs in all_obs_list}

	files_list = (getattr(obs, 'files') for obs in all_obs_list)
	response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)

	host_strs = ('pot1', 'pot2', 'pot3', 'folio', 'pot8', 'nas1')
	filetype_strs = ('uv', 'uvcRRE', 'npz')

	file_map = {host_str: {filetype: [] for filetype_str in filetype_strs} for host_str in host_strs}
	file_count = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

	for file_obj in response:
		host = getattr(file_obj, 'host')
		filetype = getattr(file_obj, 'filetype')

		obsnum = getattr(file_obj, 'obsnum')
		full_path = getattr(file_obj, 'full_path')

		file_map[host][filetype].append({'file_day': obs_dict[obsnum]['julian_day'], 'obsnum': obsnum})
		file_count[host][filetype]['file_count'] += 1

	#error_counts, error_count = get_error_counts(start_gps, end_gps)

	return render_template('histogram.html', error_counts=error_counts, host_strs=host_strs, filetype_strs=filetype_strs,
							file_count=file_count, file_map=file_map, range_start=start_time_str, range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
