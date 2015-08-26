from flask import render_template, flash, redirect, url_for, request, g, make_response
from flask.ext.login import current_user
from app.flask_app import app, db
from app import models, db_utils, histogram_utils, data_sources
from datetime import datetime
import os
import paperdata_dbi as pdbi
import pyganglia as pyg
import time
import jdcal

def time_val(value):
	#determines how much time to divide by
	time_val = 1 if value < 500 else 60 if value < 3600 else 3600 if value < 86400 else 86400
	return value / time_val

def str_val(value):
	#determines which time segment to use
	str_val = 'seconds' if value < 500 else 'minutes' if value < 3600 else 'hours' if value < 86400 else 'days'
	str_val = ' '.join(str_val, 'ago')
	return str_val

@app.route('/')
@app.route('/index')
@app.route('/index/set/<setName>')
@app.route('/set/<setName>')
def index(setName = None):
	active_data_sources = []

	if g.user is not None and g.user.is_authenticated():
		active_data_sources = g.user.active_data_sources

	if setName is not None:
		the_set = db_utils.query(database='eorlive', table='set', field_tuples=(('name', '==', setName),))[0]

		if the_set is not None:
			start_datetime, end_datetime = db_utils.get_datetime_from_utc(the_set.start, the_set.end)
			start_time_str_full = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
			end_time_str_full = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
			start_time_str_short = start_datetime.strftime('%Y/%m/%d %H:%M')
			end_time_str_short = end_datetime.strftime('%Y/%m/%d %H:%M')

			return render_template('index.html', the_set=the_set,
									start_time_str_full=start_time_str_full,
									end_time_str_full=end_time_str_full,
									start_time_str_short=start_time_str_short,
									end_time_str_short=end_time_str_short,
									active_data_sources=active_data_sources)
		else:
			flash('That set doesn\'t exist', 'error')

	return render_template('index.html', active_data_sources=active_data_sources)

@app.route('/get_graph')
def get_graph():
	graph_type_str = request.args.get('graphType')
	if graph_type_str is None:
		return make_response('No graph type', 500)

	data_source_str = request.args.get('dataSource')
	if data_source_str is None:
		return make_response('No data source', 500)

	data_source = db_utils.query(database='eorlive', table='graph_data_source', field_tuples=(('name', '==', data_source_str),))[0]

	set_str = request.args.get('set')

	template_name = ''.join(('js/', graph_type_str.lower(), '.js'))

	if set_str is None: # There should be a date range instead.
		start_time_str = request.args.get('start')
		end_time_str = request.args.get('end')
		if start_time_str is None or end_time_str is None:
			return make_response('No date range specified', 500)

		start_datetime = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%SZ')
		end_datetime = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ')

		start_utc, end_utc = db_utils.get_utc_from_datetime(start_datetime, end_datetime)

		if graph_type_str == 'Obs_File':
			return histogram_utils.get_obs_file_histogram(start_utc, end_utc, start_time_str, end_time_str)
		else:
			graph_data = data_sources.get_graph_data(data_source_str, start_utc, end_utc, None)
			data_source_str_nospace = data_source_str.replace(' ', 'ಠ_ಠ')
			return render_template('graph.html',
									data_source_str=data_source_str, graph_data=graph_data,
									plot_bands=[], template_name=template_name, is_set=False,
									data_source_str_nospace=data_source_str_nospace,
									start_time_str_short=start_datetime.strftime('%Y-%m-%d %H:%M'),
									end_time_str_short=end_datetime.strftime('%Y-%m-%d %H:%M'),
									width_slider=data_source.width_slider)
	else:
		the_set = db_utils.query(database='eorlive', table='set', field_tuples=(('name', '==', set_str),))[0]

		if the_set is None:
			return make_response('Set not found', 500)

		plot_bands = histogram_utils.get_plot_bands(the_set)

		set_start, set_end = getattr(the_set, 'start'), getattr(the_set, 'end')
		start_datetime, end_datetime = db_utils.get_datetime_from_utc(set_start, set_end)

		start_time_str_short = start_datetime.strftime('%Y-%m-%d %H:%M')
		end_time_str_short = end_datetime.strftime('%Y-%m-%d %H:%M')

		if graph_type_str == 'Obs_File':
			set_polarization, set_era_type = getattr(the_set, 'polarization'), getattr(the_set, 'era_type')
			set_host, set_filetype = getattr(the_set, 'host'), getattr(the_set, 'filetype')
			obs_count, obs_map = histogram_utils.get_observation_counts(set_start, set_end, set_polarization, set_era_type)
			file_count, file_map = histogram_utils.get_file_counts(set_start, set_end, set_host=set_host, set_filetype=set_filetype)
			range_end = end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') # For the function in histogram_utils.js
			which_data_set = data_sources.which_data_set(the_set)
			return render_template('setView.html', the_set=the_set, is_set=True,
									obs_count=obs_count, obs_map=obs_map, file_count=file_count, file_map=file_map,
									plot_bands=plot_bands, start_time_str_short=start_time_str_short,
									end_time_str_short=end_time_str_short, range_end=range_end,
									which_data_set=which_data_set)
		else:
			graph_data = data_sources.get_graph_data(data_source_str, set_start, set_end, the_set)
			data_source_str_nospace = data_source_str.replace(' ', 'ಠ_ಠ')
			which_data_set = data_sources.which_data_set(the_set)
			return render_template('graph.html',
									data_source_str=data_source_str, graph_data=graph_data, plot_bands=plot_bands,
									template_name=template_name, is_set=True, data_source_str_nospace=data_source_str_nospace,
									width_slider=data_source.width_slider, the_set=the_set,
									which_data_set=which_data_set, start_time_str_short=start_time_str_short,
									end_time_str_short=end_time_str_short)

@app.route('/data_amount', methods = ['GET'])
def data_amount():
	data = db_utils.query(database='eorlive', table='data_amount', sort_tuples=(('created_on', 'desc'),))[0]

	data_time = hours_sadb = hours_paperdata = hours_with_data = 'N/A'

	if data is not None:
		data_time = getattr(data, 'created_on')
		hours_sadb = getattr(data, 'hours_sadb')
		hours_paperdata = getattr(data, 'hours_paperdata')
		hours_with_data = getattr(data, 'hours_with_data')

	return render_template('data_amount_table.html', data_time=data_time,
							hours_sadb=hours_sadb, hours_paperdata=hours_paperdata,	hours_with_data=hours_with_data, data_time=data_time)

@app.route('/source_table', methods = ['GET'])
def source_table():
	sort_tuples = (('timestamp', 'desc'),)
	output_vars = ('timestamp', 'julian_day')
	
	corr_source = db_utils.query(database='paperdata', table='rtp_file', field_tuples=(('transferred', '==', None),),
									sort_tuples=sort_tuples, output_vars=output_vars)[0]

	rtp_source = db_utils.query(database='paperdata', table='rtp_file',	field_tuples=(('transferred', '==', True),),
								sort_tuples=sort_tuples, output_vars=output_vars)[0]

	paper_source = db_utils.query(database='paperdata', table='observation', sort_tuples=sort_tuples, output_vars=output_vars)[0]

	source_tuples = (('Correlator', corr_source), ('RTP', rtp_source), ('Folio Scan', paper_source))
	source_names = (source_name for source_name, _ in source_tuples)
	source_dict = {source_name: {'time': 'N/A', 'day': 'N/A', 'time_segment': 'N/A'} for source_name in source_names}

	for source_name, source in source_tuples:
		if source is not None:
			source_time = int(time.time() - getattr(source, 'timestamp'))

			#limiting if seconds or minutes or hours shows up on last report
			source_dict[source_name]['time_segment'] = str_val(source_time)
			source_dict[source_name]['time'] = time_val(source_time)
			source_dict[source_name]['day'] = getattr(source, 'julian_day')

	return render_template('source_table.html', source_names=source_names, source_dict=source_dict)

@app.route('/filesystem', methods = ['GET'])
def filesystem():
	systems = db_utils.query(database='ganglia', table='filesystem',
								sort_tuples=(('timestamp', 'desc'), ('host', 'asc')),
								group_tuples=('host',), output_vars=('host', 'timestamp', 'percent_space', 'used_space'))
	#							group_tuples=('host',), output_vars=('host', 'timestamp', 'percent_space', 'used_space', 'ping'))

	system_names = (getattr(system, 'host') for system in systems)
	system_dict = {system_name: {'time': 'N/A', 'used_perc': 100.0, 'time_segment': 'N/A'} for system_name in system_names}
	#system_dict = {system_name: {'time': 'N/A', 'used_perc': 100.0, 'time_segment': 'N/A',
	#								'stats': 'N/A', 'free_perc': 100.0, 'used_space': 'N/A', 'ping': 'N/A'}
	#								for system_name in system_names}
	
	system_header = (('Free', None), ('File Host', None), ('Last Report', None) , ('Usage Percent', None))

	for system in systems:
		if system is not None:
			system_time = int(time.time() - getattr(system, 'timestamp'))
			system_name = getattr(system, 'host')
			used_perc = getattr(system, 'percent_space')
			this_sys = system_dict[system_name]

			#limiting if seconds or minutes or hours shows up on last report
			this_sys['time_segment'] = str_val(system_time)
			this_sys['time'] = time_val(system_time)
			this_sys['used_perc'] = used_perc

			#this_sys['stats'] = ' '.join(system_name, 'stats')
			#this_sys['free_perc'] = 100 - used_space
			#used_space = getattr(system, 'used_space') / 1024.0 ** 3 #convert to GiB
			#this_sys['used_space'] = ' '.join(str(used_space), 'GB')
			#ping = getattr(system, 'ping')
			#this_sys['ping'] = ' '.join(str(ping), 'ms')

	return render_template('filesystem_table.html', system_header=system_header, system_dict=system_dict)

@app.route('/obs_table', methods = ['POST'])
def obs_table():
	starttime = datetime.utcfromtimestamp(int(request.form['starttime']) / 1000)
	endtime = datetime.utcfromtimestamp(int(request.form['endtime']) / 1000)

	start_utc, end_utc = db_utils.get_utc_from_datetime(starttime, endtime)

	output_vars = ('obsnum', 'julian_date', 'polarization', 'length')
	response = db_utils.query(database='paperdata', table='observation', 
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
								sort_tuples=(('time_start', 'asc'),),
								output_vars=output_vars)

	log_list = [{var: getattr(obs, var) for var in output_vars} for obs in response]

	return render_template('obs_table.html', log_list=log_list, output_vars=output_vars,
							start_time=starttime.strftime('%Y-%m-%dT%H:%M:%SZ'), end_time=endtime.strftime('%Y-%m-%dT%H:%M:%SZ'))

@app.route('/file_table', methods = ['POST'])
def file_table():
	starttime = datetime.utcfromtimestamp(int(request.form['starttime']) / 1000)
	endtime = datetime.utcfromtimestamp(int(request.form['endtime']) / 1000)

	start_utc, end_utc = db_utils.get_utc_from_datetime(starttime, endtime)

	all_obs_list = db_utils.query(database='paperdata', table='observation', 
									field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
									sort_tuples=(('time_start', 'asc'),),
									output_vars=('files',))

	files_list = (getattr(obs, 'files') for obs in all_obs_list)
	file_response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)

	output_vars=('host', 'full_path', 'obsnum', 'filesize')
	log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response]

	return render_template('file_table.html', log_list=log_list, output_vars=output_vars,
							start_time=starttime.strftime('%Y-%m-%dT%H:%M:%SZ'), end_time=endtime.strftime('%Y-%m-%dT%H:%M:%SZ'))

@app.route('/rtp_summary_table', methods = ['POST'])
def rtp_summary_table():
	obs_vars = ('files',)
	file_vars = ('host', 'path', 'julian_day', 'transferred', 'new_host', 'new_path', 'timestamp')

	rtp_obs = db_utils.query(database='paperdata', table='rtp_observation', sort_tuples=(('julian_day', 'desc'),), output_vars=obs_vars)
	files_list = (getattr(obs, 'files') for obs in rtp_obs)
	rtp_files = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)

	file_list = [{var: getattr(rtp_file, var) for var in file_vars} for rtp_file in rtp_files]
	
	file_info = {}

	for rtp_file in file_list:
		sa_host_path = ':'.join(rtp_file['host'], rtp_file['path'])
		usa_host_path = ':'.join(rtp_file['new_host'], rtp_file['new_path'])

		julian_day = rtp_file['julian_day']
		transferred = rtp_file['transferred']
		timestamp = rtp_file['time_stamp']

		if julian_day in file_info.keys():
			file_info[julian_day]['count'] += 1
			file_info[julian_day]['transfer']['all'] += 1
		else:
			file_info.update({julian_day: {'count': 1, 'transfer': {'all': 1, 'moved': 0},
											'host_path': {'sa_host_path': sa_host_path, 'usa_host_path': usa_host_path},
											'activity': 'N/A', 'last_updated': timestamp, 'lst_range': 'N/A'}})
		if transferred:
			file_info[julian_day]['transfer']['moved'] += 1

	julian_days = file_info.keys()

	output_vars = ('gregorian_day', 'lst_range', 'file_count', 'sa_host_path', 'usa_host_path',
					'output_host', 'transfer_percent', 'activity', 'last_updated')
	rtp_header = (('Date', 'Observation Date'), ('JD', 'Julian Day'), ('LST Range', 'Local Sidereal Time Range'),
					('Count', 'File Count'), ('RTP status', 'Percent of Files transferred'),
					('SA Location', 'Host and Path of files on SADB'), ('USA Location', 'Host and Path of files on paperdata'),
					('Output Host', 'Host raw files are backup up'), ('Last Activity', 'Last backend action taken'),
					('Last Updated', 'Last time action was taken'))
	summary_dict = {julian_day: {var: 0 for var in output_vars} for julian_day in julian_days}
	#need a dict of julian_day: (gregorian_day, lst_range, file_count, sa, usa, output_host?, trans_perc, activity, last_updated)
	for julian_day in julian_days:
		file_dict = file_info[julian_day]
		sum_dict = summary_dict[julian_day]
		year, month, day, _ = jdcal.jd2cal(jdcal.MJD_0, julian_day - jdcal.MJD_0)
		gd = datetime(year, month, day)
		sum_dict['gregorian_day'] = gd.strftime('%Y-%m-%d') 
		sum_dict['lst_range'] = file_dict['lst_range'] 
		sum_dict['file_count'] = file_dict['count']
		sum_dict['sa_host_path'] = file_dict['host_path']['sa_host_path']
		sum_dict['usa_host_path'] = file_dict['host_path']['usa_host_path']
		sum_dict['output_host'] = file_dict['host_path']['output_host']
		sum_dict['transfer_percent'] = file_dict['transfer']['moved'] / file_dict['transfer']['all']
		sum_dict['activity'] = file_dict['activity']
		sum_dict['last_updated'] = file_dict['last_updated']
		
	return render_template('rtp_summary_table.html', rtp_header=rtp_header, summary_dict=summary_dict, output_vars=output_vars)

@app.before_request
def before_request():
	g.user = current_user
	paper_dbi = pdbi.DataBaseInterface()
	pyg_dbi = pyg.DataBaseInterface()
	try :
		g.paper_session = paper_dbi.Session()
		g.pyg_session = pyg_dbi.Session()
		g.eorlive_session = db.session
	except Exception as e:
		print('Cannot connect to database - {e}'.format(e))

@app.teardown_request
def teardown_request(exception):
	paper_db = getattr(g, 'paper_session', None)
	pyg_db = getattr(g, 'pyg_session', None)
	eorlive_db = getattr(g, 'eorlive_session', None)
	db_list = (paper_db, pyg_db, eorlive_db)
	for open_db in db_list:
		if open_db is not None:
			open_db.close()

@app.route('/profile')
def profile():
	if (g.user is not None and g.user.is_authenticated()):
		user = db_utils.query(database='eorlive', table='user',	field_tuples=(('username', '==', g.user.username),),)[0]

		setList = db_utils.query(database='eorlive', table='set', field_tuples=(('username', '==', g.user.username),))[0]

		return render_template('profile.html', user=user, sets=setList)
	else:
		return redirect(url_for('login'))

@app.route('/user_page')
def user_page():
	if (g.user is not None and g.user.is_authenticated()):
		user = db_utils.query(database='eorlive', table='user',	field_tuples=(('username', '==', g.user.username),))[0]

		userList = db_utils.query(database='eorlive', table='user')[0]

		setList = db_utils.query(database='eorlive', table='set')[0]

		return render_template('user_page.html', theUser=user, userList=userList, setList=setList)
	else:
		return redirect(url_for('login'))

@app.route('/data_summary_table', methods=['POST'])
def data_summary_table():
	#table that shows on side of website under login
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	startdatetime = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%SZ')
	enddatetime = datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%SZ')

	start_utc, end_utc = db_utils.get_utc_from_datetime(startdatetime, enddatetime)

	response = db_utils.query(database='paperdata', table='observation',
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
								sort_tuples=(('time_start', 'asc'),),
								output_vars=('time_start', 'time_end', 'polarization', 'era_type', 'files')))

	pol_strs, era_type_strs, host_strs, filetype_strs = db_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: {'obs_count': 0, 'obs_hours': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}
	file_map = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

	for obs in response:
		polarization = getattr(obs, 'polarization')
		era_type = getattr(obs, 'era_type')

		# Actual UTC time of the obs (for the graph)
		obs_start = getattr(obs, 'time_start')
		obs_end = getattr(obs, 'time_end')

		obs_map[polarization][era_type]['obs_count'] += 1
		obs_map[polarization][era_type]['obs_hours'] += (obs_end - obs_start) / 3600.0

		for paper_file in getattr(obs, 'files'):
			host = getattr(paper_file, 'host')
			filetype = getattr(paper_file, 'filetype')
			file_map[host][filetype]['file_count'] += 1

	all_obs_strs = pol_strs + era_type_strs
	obs_total = {all_obs_str: {'count': 0, 'hours': 0} for all_obs_str in all_obs_strs}

	for pol in pol_strs:
		for era_type in era_type_strs:
			obs_count = obs_map[pol][era_type]['obs_count']
			obs_hours = obs_map[pol][era_type]['obs_hours']
			obs_total[era_type]['count'] += obs_count
			obs_total[era_type]['hours'] += obs_hours
			obs_total[pol]['count'] += obs_count
			obs_total[pol]['hours'] += obs_hours

	all_file_strs = host_strs + filetype_strs
	file_total = {all_file_str: {'count': 0} for all_file_str in all_file_strs}

	for host in host_strs:
		for filetype in filetype_strs:
			file_count = file_map[host][filetype]['file_count']
			file_total[filetype]['count'] += file_count
			file_total[host]['count'] += file_count

	return render_template('summary_table.html',
							pol_strs=pol_strs, era_type_strs=era_type_strs, host_strs=host_strs, filetype_strs=filetype_strs,
							obs_map=obs_map, obs_total=obs_total, file_map=file_map, file_total=file_total)
