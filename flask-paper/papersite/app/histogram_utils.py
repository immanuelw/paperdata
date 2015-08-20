from app import db_utils, models
from flask import g, render_template
import math

def get_error_counts(start_gps, end_gps):
	error_counts = []
	error_count = 0

	obscontroller_response = db_utils.get_query_results(database='eor', table='obscontroller_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time', 'observation_number', 'comment'))
	obscontroller_response = tuple(math.floor(ref_time) for ref_time in obscontroller_response)

	recvstatuspolice_response = db_utils.get_query_results(database='eor', table='recvstatuspolice_log',
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

def get_observation_counts(start_gps, end_gps, polarization, era, era_type):
	response = db_utils.get_query_results(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_gps), ('time_end', '<=', end_gps),
										('polarization', None if polarization == 'any' else '==', polarization),
										('era', None if era == 0 else '==', era),
										('era_type', None if era_type == 'any' else '==', era_type)),
										sort_tuples=(('time_start', 'asc'),), output_vars=('time_start', 'time_end'))

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	observation_counts = []
	utc_obsid_map = []

	for observation in response:
		utc_millis = int((observation[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)
		observation_counts.append([utc_millis, 1])
		utc_obsid_map.append([utc_millis, observation[0]])

	return (observation_counts, utc_obsid_map)

def get_plot_bands(the_set):
	flagged_subsets = db_utils.get_query_results(database='eorlive', table='flagged_subset',
													field_tuples=(('set_id', '==', getattr(the_set, 'id')),))

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	plot_bands = [{'from': int((getattr(flagged_subset, 'start') - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000),
					'to': int((getattr(flagged_subset, 'end') - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000),
					'color': 'yellow'}
					for flagged_subset in flagged_subsets]

	return plot_bands

def get_obs_err_histogram(start_gps, end_gps, start_time_str, end_time_str):
	response = db_utils.get_query_results(database='paperdata', table='observation',
										field_tuples=(('time_start', '>=', start_gps), ('time_end', '<=', end_gps),
										sort_tuples=(('time_start', 'asc'),),
										output_vars=('time_start', 'polarization', 'era', 'era_type', 'obsnum'))

	pol_strs, era_strs, era_type_strs = db.utils.set_strings()
	obs_counts = {'{pol_str}-{era_str}-{era_type_str}'.format(pol_str=pol_str, era_str=era_str, era_type_str=era_type_str):[]
														for pol_str in pol_strs	for era_str in era_strs for era_type_str in era_type_strs}
	utc_obs_map = {'{pol_str}-{era_str}-{era_type_str}'.format(pol_str=pol_str, era_str=era_str, era_type_str=era_type_str):[]
														for pol_str in pol_strs	for era_str in era_strs for era_type_str in era_type_strs}

	error_counts, error_count = get_error_counts(start_gps, end_gps)

	for observation in response:
		# Actual UTC time of the observation (for the graph)
		obs_time = getattr(observation, 'time_start')

		polarization = getattr(observation, 'polarization')
		era = getattr(observation, 'era')
		era_type = getattr(observation, 'era_type')
		obsnum = getattr(observation, 'obsnum')

		pol_era = '{polarization}-{era}-{era_type}'.format(polarization=polarization, era=era, era_type=era_type)

		obs_counts[pol_era].append((obs_time, 1))
		utc_obs_map[pol_era].append((obs_time, obsnum))

	return render_template('histogram.html', error_counts=error_counts,
							low_eor0_counts=low_eor0_counts, high_eor0_counts=high_eor0_counts,
							low_eor1_counts=low_eor1_counts, high_eor1_counts=high_eor1_counts,
							utc_obsid_map_l0=utc_obsid_map_l0, utc_obsid_map_l1=utc_obsid_map_h0,
							utc_obsid_map_h0=utc_obsid_map_l1, utc_obsid_map_h1=utc_obsid_map_h1,
							range_start=start_time_str,	range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
