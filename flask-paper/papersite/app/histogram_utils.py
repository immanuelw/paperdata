from app import db_utils, models
from flask import g, render_template
import math

def get_error_counts(start_gps, end_gps):
	error_counts = []
	error_count = 0

	obscontroller_response = db_utils.get_query_results(data_source=None, database='eor', table='obscontroller_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time', 'observation_number', 'comment'))
	obscontroller_response = tuple(math.floor(ref_time) for ref_time in obscontroller_response)

	recvstatuspolice_response = db_utils.get_query_results(data_source=None, database='eor', table='recvstatuspolice_log',
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

def get_observation_counts(start_gps, end_gps, low_or_high, eor):
	response = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
										(('starttime', '>=', start_gps), ('starttime', '<=', end_gps),
										('obsname', None if low_or_high == 'any' else 'like', ''.join(low_or_high, '%')),
										('ra_phase_center', None if eor == 'any' else '==', 0 if eor == 'EOR0' else 60))
										sort_tuples=(('starttime', 'asc'),), output_vars=('starttime', 'obsname', 'ra_phase_center'))

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	observation_counts = []
	utc_obsid_map = []

	for observation in response:
		utc_millis = int((observation[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)
		observation_counts.append([utc_millis, 1])
		utc_obsid_map.append([utc_millis, observation[0]])

	return (observation_counts, utc_obsid_map)

def get_plot_bands(the_set):
	flagged_subsets = db_utils.get_query_results(data_source=None, database='eorlive', table='flagged_subset',
													field_tuples=(('set_id', '==', getattr(the_set, 'id')),),
													sort_tuples=None, output_vars=None)

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	plot_bands = [{'from': int((getattr(flagged_subset, 'start') - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000),
					'to': int((getattr(flagged_subset, 'end') - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000),
					'color': 'yellow'}
					for flagged_subset in flagged_subsets]

	return plot_bands

def get_obs_err_histogram(start_gps, end_gps, start_time_str, end_time_str):
	response = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
										(('starttime', '>=', start_gps), ('starttime', '<=', end_gps),
										sort_tuples=(('starttime', 'asc'),),
										output_vars=('starttime', 'stoptime', 'obsname', 'ra_phase_center'))

	low_eor0_counts = []

	high_eor0_counts = []

	low_eor1_counts = []

	high_eor1_counts = []

	error_counts, error_count = get_error_counts(start_gps, end_gps)

	utc_obsid_map_l0 = []
	utc_obsid_map_l1 = []
	utc_obsid_map_h0 = []
	utc_obsid_map_h1 = []

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	for observation in response:
						# Actual UTC time of the observation (for the graph)
		utc_millis = int((observation[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)

		obs_name = observation[2]

		try:
			ra_phase_center = int(observation[3])
		except TypeError as te:
			ra_phase_center = -1

		if 'low' in obs_name:
			if ra_phase_center == 0: # EOR0
				low_eor0_counts.append([utc_millis, 1])
				utc_obsid_map_l0.append([utc_millis, int(observation[0])])
			elif ra_phase_center == 60: # EOR1
				low_eor1_counts.append([utc_millis, 1])
				utc_obsid_map_l1.append([utc_millis, int(observation[0])])
		elif 'high' in obs_name:
			if ra_phase_center == 0: # EOR0
				high_eor0_counts.append([utc_millis, 1])
				utc_obsid_map_h0.append([utc_millis, int(observation[0])])
			elif ra_phase_center == 60: # EOR1
				high_eor1_counts.append([utc_millis, 1])
				utc_obsid_map_h1.append([utc_millis, int(observation[0])])

	return render_template('histogram.html',
		low_eor0_counts=low_eor0_counts, high_eor0_counts=high_eor0_counts,
		low_eor1_counts=low_eor1_counts, high_eor1_counts=high_eor1_counts,
		error_counts=error_counts, utc_obsid_map_l0=utc_obsid_map_l0,
		utc_obsid_map_l1=utc_obsid_map_l1, utc_obsid_map_h0=utc_obsid_map_h0,
		utc_obsid_map_h1=utc_obsid_map_h1, range_start=start_time_str,
		range_end=end_time_str, start_time_str_short=start_time_str.replace('T', ' ')[0:16],
		end_time_str_short=end_time_str.replace('T', ' ')[0:16])
