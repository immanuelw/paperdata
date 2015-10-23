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
import paper as ppdata
from paper.data import dbi as pdbi
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

def get_observation_counts(start_utc, end_utc, pol_strs=None, era_type_strs=None, set_pol='all', set_era_type='all'):
	'''
	get all observations to plot

	Parameters
	----------
	start_utc | int: start time in utc
	end_utc | int: end time in utc
	pol_strs | Optional[list]: polarizations --defaults to None
	era_type_strs | Optional[list]: era types --defaults to None
	set_pol | str: set polarization --defaults to 'all'
	set_era_type | str: set era type --defaults to 'all'

	Returns
	-------
	tuple:
		dict: 2D dict of count by polarization and era type
		dict: 2D dict of lists of dicts of time and obsnum by polarization and era type
	'''
	if (pol_strs is None) or (era_type_strs is None):
		pol_strs, era_type_strs, _, _ = misc_utils.get_set_strings()
	try:
		response = db_utils.query(database='paperdata', table='Observation',
									field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
									('polarization', None if set_pol == 'all' else '==', set_pol),
									('era_type', None if set_era_type == 'all' else '==', set_era_type)),
									sort_tuples=(('time_start', 'asc'),))
	except:
		response = (None,)

	pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: [] for era_type_str in era_type_strs} for pol_str in pol_strs}

	for obs in response:
		if obs is not None:
			obs_map[obs.polarization][str(obs.era_type)].append({'obs_time': obs.time_start, 'obsnum': obs.obsnum})
	obs_count = {pol_str: {era_type_str: len(obs_map[pol_str][era_type_str]) for era_type_str in era_type_strs} for pol_str in pol_strs}

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
	if (host_strs is None) or (filetype_strs is None):
		_, _, host_strs, filetype_strs = misc_utils.get_set_strings()

	file_map = {host_str: {filetype_str: [] for filetype_str in filetype_strs} for host_str in host_strs}

	print(start_utc, end_utc)
	try:
		try:
			rtp_obs_list = db_utils.query(database='paperdata', table='RTPObservation',
											field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)))
		except:
			rtp_obs_list = []

		dbi = pdbi.DataBaseInterface()
		with dbi.session_scope() as s:
			obs_table = pdbi.Observation
			obs_response = s.query(obs_table).filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc).all()

			for obs in obs_response:
				for file_obj in obs.files:
					if file_obj is not None:
						if (set_host and set_host != host) and (set_filetype and set_filetype != filetype):
							continue
						file_map[file_obj.host][file_obj.filetype].append({'file_date': file_obj.observation.julian_date,
																			'obsnum': file_obj.obsnum})
			file_count = {host_str: {filetype_str: len(file_map[host_str][filetype_str])
							for filetype_str in filetype_strs} for host_str in host_strs}
	except:
		file_count, file_map = (None, None)

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
	pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
	obs_count, obs_map = get_observation_counts(start_utc, end_utc, pol_strs, era_type_strs)
	file_count, file_map = get_file_counts(start_utc, end_utc, host_strs, filetype_strs)

	file_map = json.dumps(file_map, default=ppdata.decimal_default)
	file_count = json.dumps(file_count)
	obs_map = json.dumps(obs_map, default=ppdata.decimal_default)
	obs_count = json.dumps(obs_count)

	return render_template('histogram.html', pol_strs=list(pol_strs), era_type_strs=list(era_type_strs),
							host_strs=list(host_strs), filetype_strs=list(filetype_strs),
							file_map=file_map, file_counts=file_count, obs_map=obs_map, obs_counts=obs_count,
							file_dict=file_map, obs_dict=obs_map,
							range_start=start_time_str, range_end=end_time_str,
							start_time_str_short=start_time_str.replace('T', ' ')[0:16], end_time_str_short=end_time_str.replace('T', ' ')[0:16])
