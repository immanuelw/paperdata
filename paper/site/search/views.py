from flask import render_template, flash, redirect, url_for, request, g, make_response
from flask.ext.login import current_user
import os
import time
import json
from datetime import datetime
from paper.site.flask_app import search_app as app, search_db as db
from paper.site.search import models, histogram_utils, data_sources
from paper.site import db_utils, misc_utils
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg

def time_val(value):
	'''
	determines how much time to divide by and divides time by that to make human readable

	input: numerical time value
	output: numerical time value divided
	'''
	time_val = 1 if value < 500 else 60 if value < 3600 else 3600 if value < 86400 else 86400
	return value / time_val

def str_val(value):
	'''
	determines which time unit to use

	input: numerical time value
	output: time unit string
	'''
	str_val = 'seconds' if value < 500 else 'minutes' if value < 3600 else 'hours' if value < 86400 else 'days'
	str_val = ' '.join(str_val, 'ago')
	return str_val

@app.route('/')
@app.route('/index')
@app.route('/index/set/<setName>')
@app.route('/set/<setName>')
def index(setName=None):
	'''
	start page of the website
	pull in set name if possible to pregenerate set information

	input: set name
	output: index html
	'''
	active_data_sources = []

	if g.user is not None and g.user.is_authenticated():
		active_data_sources = g.user.active_data_sources

	if setName is not None:
		the_set = db_utils.query(database='search', table='set', field_tuples=(('name', '==', setName),))[0]

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
	'''
	get graph information or set view

	output: graph html or set view html
	'''
	graph_type_str = request.args.get('graphType')
	if graph_type_str is None:
		return make_response('No graph type', 500)

	data_source_str = request.args.get('dataSource')
	if data_source_str is None:
		return make_response('No data source', 500)
	try:
		data_source = db_utils.query(database='search', table='graph_data_source', field_tuples=(('name', '==', data_source_str),))[0]
	except IndexError:
		data_source = None
		#return make_response('Source not found', 500)

	set_str = request.args.get('set')

	template_name = ''.join(('js/', graph_type_str.lower(), '.js'))

	if set_str is None: # There should be a date range instead.
		start_time_str = request.args.get('start')
		end_time_str = request.args.get('end')
		if start_time_str is None or end_time_str is None:
			return make_response('No date range specified', 500)

		start_datetime = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%SZ')
		end_datetime = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ')

		start_utc = misc_utils.get_jd_from_datetime(start_datetime)
		end_utc = misc_utils.get_jd_from_datetime(start_datetime)

		if graph_type_str == 'Obs_File':
			return histogram_utils.get_obs_file_histogram(start_utc, end_utc, start_time_str, end_time_str)
		else:
			return make_response('No data source', 500)
			#graph_data = data_sources.get_graph_data(data_source_str, start_utc, end_utc, None)
			#data_source_str_nospace = data_source_str.replace(' ', 'ಠ_ಠ')
			#return render_template('graph.html',
			#						data_source_str=data_source_str, graph_data=graph_data,
			#						plot_bands=[], template_name=template_name, is_set=False,
			#						data_source_str_nospace=data_source_str_nospace,
			#						start_time_str_short=start_datetime.strftime('%Y-%m-%d %H:%M'),
			#						end_time_str_short=end_datetime.strftime('%Y-%m-%d %H:%M'),
			#						width_slider=data_source.width_slider)
	else:
		try:
			the_set = db_utils.query(database='search', table='set', field_tuples=(('name', '==', set_str),))[0]
		except:
			the_set = None

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
			obs_count = json.dumps(obs_count)
			obs_map = json.dumps(obs_map)
			file_count = json.dumps(file_count)
			file_map = json.dumps(file_map)
			return render_template('setView.html', the_set=the_set, is_set=True,
									obs_counts=obs_count, obs_map=obs_map, file_counts=file_count, file_map=file_map,
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

@app.route('/obs_table', methods = ['POST'])
def obs_table():
	'''
	generate observation table for histogram bar

	output: observation table html
	'''
	starttime = datetime.utcfromtimestamp(int(request.form['starttime']) / 1000)
	endtime = datetime.utcfromtimestamp(int(request.form['endtime']) / 1000)

	start_utc = misc_utils.get_jd_from_datetime(starttime)
	end_utc = misc_utils.get_jd_from_datetime(endtime)

	output_vars = ('obsnum', 'julian_date', 'polarization', 'length')
	try:
		response = db_utils.query(database='paperdata', table='observation', 
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
								sort_tuples=(('time_start', 'asc'),))
		log_list = [{var: getattr(obs, var) for var in output_vars} for obs in response]
	except:
		log_list = []

	return render_template('obs_table.html', log_list=log_list, output_vars=output_vars,
							start_time=starttime.strftime('%Y-%m-%dT%H:%M:%SZ'), end_time=endtime.strftime('%Y-%m-%dT%H:%M:%SZ'))

@app.route('/file_table', methods = ['POST'])
def file_table():
	'''
	generate file table for histogram bar

	output: file table html
	'''
	starttime = datetime.utcfromtimestamp(int(request.form['starttime']) / 1000)
	endtime = datetime.utcfromtimestamp(int(request.form['endtime']) / 1000)

	start_utc = misc_utils.get_jd_from_datetime(starttime)
	end_utc = misc_utils.get_jd_from_datetime(endtime)

	output_vars=('host', 'full_path', 'obsnum', 'filesize')

	try:
		all_obs_list = db_utils.query(database='paperdata', table='observation', 
										field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
										sort_tuples=(('time_start', 'asc'),))
		files_list = (getattr(obs, 'files') for obs in all_obs_list)
		file_response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)

		log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response]
	except:
		log_list = []

	return render_template('file_table.html', log_list=log_list, output_vars=output_vars,
							start_time=starttime.strftime('%Y-%m-%dT%H:%M:%SZ'), end_time=endtime.strftime('%Y-%m-%dT%H:%M:%SZ'))

@app.before_request
def before_request():
	'''
	access database as user before request
	'''
	g.user = current_user
	try:
		paper_dbi = pdbi.DataBaseInterface()
		pyg_dbi = pyg.DataBaseInterface()
	except:
		paper_dbi = pdbi.DataBaseInterface(configfile='/mnt/paperdata/paperdata.cfg')
		pyg_dbi = pyg.DataBaseInterface(configfile='/mnt/paperdata/ganglia.cfg')
	try :
		g.paper_session = paper_dbi.Session()
		g.pyg_session = pyg_dbi.Session()
		g.search_session = db.session
	except Exception as e:
		print('Cannot connect to database - {e}'.format(e))

@app.teardown_request
def teardown_request(exception):
	'''
	exit database after request

	input: exception
	'''
	paper_db = getattr(g, 'paper_session', None)
	pyg_db = getattr(g, 'pyg_session', None)
	search_db = getattr(g, 'search_session', None)
	db_list = (paper_db, pyg_db, search_db)
	for open_db in db_list:
		if open_db is not None:
			open_db.close()

@app.route('/profile')
def profile():
	'''
	access user profile

	output: profile html or redirect for login
	'''
	if (g.user is not None and g.user.is_authenticated()):
		try:
			user = db_utils.query(database='search', table='user',	field_tuples=(('username', '==', g.user.username),),)[0]
			setList = db_utils.query(database='search', table='set', field_tuples=(('username', '==', g.user.username),))[0]
		except:
			user = (None,)
			setList = (None,)

		return render_template('profile.html', user=user, sets=setList)
	else:
		return redirect(url_for('login'))

@app.route('/user_page')
def user_page():
	'''
	access user page

	output: user page html or redirect for login
	'''
	if (g.user is not None and g.user.is_authenticated()):
		try:
			user = db_utils.query(database='search', table='user',	field_tuples=(('username', '==', g.user.username),))[0]
			userList = db_utils.query(database='search', table='user')[0]
			setList = db_utils.query(database='search', table='set')[0]
		except:
			user = (None,)
			userList = (None,)
			setList = (None,)

		return render_template('user_page.html', theUser=user, userList=userList, setList=setList)
	else:
		return redirect(url_for('login'))

@app.route('/data_summary_table', methods=['POST'])
def data_summary_table():
	'''
	summary of data in main databases

	output: summary table html
	'''
	#table that shows on side of website under login
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	startdatetime = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%SZ')
	enddatetime = datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%SZ')

	start_utc =  misc_utils.get_jd_from_datetime(startdatetime)
	end_utc =  misc_utils.get_jd_from_datetime(enddatetime)

	response = db_utils.query(database='paperdata', table='observation',
								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
								sort_tuples=(('time_start', 'asc'),))
	#try:
	#	response = db_utils.query(database='paperdata', table='observation',
	#								field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
	#								sort_tuples=(('time_start', 'asc'),))
	#except:
	#	response = (None,)

	pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: {'obs_count': 0, 'obs_hours': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}
	file_map = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

	for obs in response:
		if not obs is None:
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