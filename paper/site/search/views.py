'''
paper.site.search.views

author | Immanuel Washington

Functions
---------
time_fix | fixes times for observation input
index | shows main page
get_graph | plots graph
obs_table | shows observation table
save_obs | generates observation json file
file_table | shows file table
save_files | generates file json file
before_request | accesses database
teardown_request | exits database
profile | shows user profile
user_page | shows user page
data_summary_table | shows data summary table
day_summary_table | shows day summary table
'''
from flask import render_template, flash, redirect, url_for, request, g, make_response, Response
from flask.ext.login import current_user
import json
from datetime import datetime
import paper as ppdata
from paper.site.flask_app import search_app as app, search_db as db
from paper.site.search import models, histogram_utils, data_sources
from paper.site import db_utils, misc_utils
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg
from sqlalchemy import func

def time_fix(jdstart, jdend, starttime=None, endtime=None):
	'''
	fixes times for observations

	Parameters
	----------
	jdstart | str: starting time of julian date
	jdend | str: ending time of julian date
	starttime | str: string of start time --defaults to None
	endtime | str: string of end time --defaults to None

	Returns
	-------
	tuple:
		float(5): julian date start time
		float(5): julian date end time
	'''
	try:
		jd_start = round(float(jdstart), 5)
		jd_end = round(float(jdend), 5)
	except:
		jd_start = None
		jd_end = None

	if jd_start == None:
		startdatetime = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%SZ')
		enddatetime = datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%SZ')
		start_utc, end_utc = misc_utils.get_jd_from_datetime(startdatetime, enddatetime)
	else:
		start_utc, end_utc = jd_start, jd_end

	return start_utc, end_utc

@app.route('/')
@app.route('/index')
@app.route('/index/set/<setName>')
@app.route('/set/<setName>')
def index(setName=None):
	'''
	start page of the website
	pull in set name if possible to pregenerate set information

	Parameters
	----------
	setName | str: set name --defaults to None

	Returns
	-------
	html: index
	'''
	active_data_sources = []
	polarization_dropdown, era_type_dropdown, host_dropdown, filetype_dropdown = misc_utils.get_dropdowns()
	if g.user is not None and g.user.is_authenticated():
		active_data_sources = g.user.active_data_sources

	if setName is not None:
		the_set = db_utils.query(database='search', table='Set', field_tuples=(('name', '==', setName),))[0]

		if the_set is not None:
			start_datetime, end_datetime = db_utils.get_datetime_from_utc(the_set.start, the_set.end)
			start_time_str_full = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
			end_time_str_full = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
			start_time_str_short = start_datetime.strftime('%Y/%m/%d %H:%M')
			end_time_str_short = end_datetime.strftime('%Y/%m/%d %H:%M')

			return render_template('index.html', the_set=the_set,
									start_time_str_full=start_time_str_full, end_time_str_full=end_time_str_full,
									start_time_str_short=start_time_str_short, end_time_str_short=end_time_str_short,
									active_data_sources=active_data_sources,
									polarization_dropdown=polarization_dropdown, era_type_dropdown=era_type_dropdown,
									host_dropdown=host_dropdown, filetype_dropdown=filetype_dropdown)
		else:
			flash('That set doesn\'t exist', 'error')

	return render_template('index.html', active_data_sources=active_data_sources,
							polarization_dropdown=polarization_dropdown, era_type_dropdown=era_type_dropdown,
							host_dropdown=host_dropdown, filetype_dropdown=filetype_dropdown)

@app.route('/get_graph')
def get_graph():
	'''
	get graph information or set view

	Returns
	-------
	html: graph
	OR
	html: set view
	'''
	graph_type_str = request.args.get('graphType')
	if graph_type_str is None:
		return make_response('No graph type', 500)

	data_source_str = request.args.get('dataSource')
	if data_source_str is None:
		return make_response('No data source', 500)
	try:
		data_source = db_utils.query(database='search', table='GraphDataSource', field_tuples=(('name', '==', data_source_str),))[0]
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

		start_utc, end_utc = misc_utils.get_jd_from_datetime(start_datetime, end_datetime)

		if graph_type_str == 'Obs_File':
			return histogram_utils.get_obs_file_histogram(start_utc, end_utc, start_time_str, end_time_str)
		else:
			return make_response('No data source', 500)
			#graph_data = data_sources.get_graph_data(data_source_str, start_utc, end_utc, None)
			#data_source_str_nospace = data_source_str.replace(' ', 'NNN')
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

		set_start, set_end = the_set.start, the_set.end
		start_datetime, end_datetime = db_utils.get_datetime_from_utc(set_start, set_end)

		start_time_str_short = start_datetime.strftime('%Y-%m-%d %H:%M')
		end_time_str_short = end_datetime.strftime('%Y-%m-%d %H:%M')

		if graph_type_str == 'Obs_File':
			set_polarization, set_era_type = the_set.polarization, the_set.era_type
			set_host, set_filetype = the_set.host, the_set.filetype
			obs_count, obs_map = histogram_utils.get_observation_counts(set_start, set_end, set_pol=set_polarization, set_era_type=set_era_type)
			file_count, file_map = histogram_utils.get_file_counts(set_start, set_end, set_host=set_host, set_filetype=set_filetype)
			range_end = end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') # For the function in histogram_utils.js
			obs_count = json.dumps(obs_count)
			obs_map = json.dumps(obs_map, default=ppdata.decimal_default)
			file_count = json.dumps(file_count)
			file_map = json.dumps(file_map, default=ppdata.decimal_default)
			return render_template('setView.html', the_set=the_set, is_set=True,
									obs_counts=obs_count, obs_map=obs_map, file_counts=file_count, file_map=file_map,
									plot_bands=plot_bands, start_time_str_short=start_time_str_short,
									end_time_str_short=end_time_str_short, range_end=range_end)
		else:
			graph_data = data_sources.get_graph_data(data_source_str, set_start, set_end, the_set)
			data_source_str_nospace = data_source_str.replace(' ', 'NNN')
			return render_template('graph.html',
									data_source_str=data_source_str, graph_data=graph_data, plot_bands=plot_bands,
									template_name=template_name, is_set=True, data_source_str_nospace=data_source_str_nospace,
									width_slider=data_source.width_slider, the_set=the_set, start_time_str_short=start_time_str_short,
									end_time_str_short=end_time_str_short)

@app.route('/obs_table', methods = ['POST'])
def obs_table():
	'''
	generate observation table for histogram bar

	Returns
	-------
	html: observation table
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	polarization = request.form['polarization']
	era_type = request.form['era_type']

	fixed_et = None if era_type == 'None' else era_type
	output_vars = ('obsnum', 'julian_date', 'polarization', 'length')

	try:
		response = db_utils.query(database='paperdata', table='Observation', 
									field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
									('polarization', None if polarization == 'any' else '==', polarization),
									('era_type', None if era_type in ('all', 'None') else '==', era_type)),
									sort_tuples=(('time_start', 'asc'),))
		log_list = [{var: getattr(obs, var) for var in output_vars} for obs in response]
	except:
		log_list = []

	return render_template('obs_table.html', log_list=log_list, output_vars=output_vars, start_time=starttime, end_time=endtime)

@app.route('/save_obs', methods = ['POST'])
def save_obs():
	'''
	saves observations as json

	Returns
	-------
	html: json file
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	polarization = request.form['polarization']
	era_type = request.form['era_type']

	fixed_et = None if era_type == 'None' else era_type

	try:
		response = db_utils.query(database='paperdata', table='Observation', 
									field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
									('polarization', None if polarization == 'any' else '==', polarization),
									('era_type', None if era_type in ('all', 'None') else '==', era_type)),
									sort_tuples=(('time_start', 'asc'),))

		entry_list = [obs.to_dict() for obs in response]
	except:
		entry_list = []

	return Response(response=json.dumps(entry_list, sort_keys=True, indent=4, default=ppdata.decimal_default),
					status=200, mimetype='application/json', headers={'Content-Disposition': 'attachment; filename=obs.json'})

@app.route('/file_table', methods = ['POST'])
def file_table():
	'''
	generate file table for histogram bar

	Returns
	-------
	html: file table
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	host = request.form['host']
	filetype = request.form['filetype']

	output_vars = ('host', 'source', 'obsnum', 'filesize')

	try:
		dbi = pdbi.DataBaseInterface()
		table = pdbi.Observation
		with dbi.session_scope() as s:
			all_obs_list = s.query(table).filter(table.time_start >= start_utc).filter(table.time_end <= end_utc)\
											.order_by(table.time_start.asc()).all()
			files_list = (obs.files for obs in all_obs_list)
			file_response = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)

			if host == 'all' and filetype == 'all':
				log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response]
			elif host == 'all' and filetype != 'all':
				log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response
							if paper_file.filetype == filetype]
			elif host != 'all' and filetype == 'all':
				log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response
							if paper_file.host == host]
			else:
				log_list = [{var: getattr(paper_file, var) for var in output_vars} for paper_file in file_response
							if paper_file.host == host and paper_file.filetype == filetype]
	except:
		log_list = []

	return render_template('file_table.html', log_list=log_list, output_vars=output_vars, start_time=starttime, end_time=endtime)

@app.route('/save_files', methods = ['POST'])
def save_files():
	'''
	saves file metadata as json

	Returns
	-------
	html: json file
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	host = request.form['host']
	filetype = request.form['filetype']

	try:
		dbi = pdbi.DataBaseInterface()
		table = pdbi.Observation
		with dbi.session_scope() as s:
			all_obs_list = s.query(table).filter(table.time_start >= start_utc).filter(table.time_end <= end_utc)\
											.order_by(table.time_start.asc()).all()
			file_response = (file_obj for obs in all_obs_list for file_obj in obs.files)

			entry_list = [paper_file.to_dict() for paper_file in file_response if paper_file.host == host and paper_file.filetype == filetype]
	except:
		entry_list = []

	return Response(response=json.dumps(entry_list, sort_keys=True, indent=4, default=ppdata.decimal_default),
					status=200, mimetype='application/json', headers={'Content-Disposition': 'attachment; filename=file.json'})

@app.before_request
def before_request():
	'''
	access database as user before request
	'''
	g.user = current_user
	paper_dbi = pdbi.DataBaseInterface()
	pyg_dbi = pyg.DataBaseInterface()
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

	Parameters
	----------
	exception (exception): exception
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

	Returns
	-------
	html: profile
	OR
	html: redirect for login
	'''
	if (g.user is not None and g.user.is_authenticated()):
		try:
			user = db_utils.query(database='search', table='User', field_tuples=(('username', '==', g.user.username),),)[0]
			setList = db_utils.query(database='search', table='Set', field_tuples=(('username', '==', g.user.username),))[0]
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

	Returns
	-------
	html: user page
	OR
	html: redirect for login
	'''
	if (g.user is not None and g.user.is_authenticated()):
		try:
			user = db_utils.query(database='search', table='User', field_tuples=(('username', '==', g.user.username),))[0]
			userList = db_utils.query(database='search', table='User')[0]
			setList = db_utils.query(database='search', table='Set')[0]
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

	Returns
	-------
	html: data summary table
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
	obs_map = {pol_str: {era_type_str: {'obs_count': 0, 'obs_hours': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}
	file_map = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		obs_table = pdbi.Observation
		response = s.query(obs_table.polarization, obs_table.era_type, func.sum(obs_table.length), func.count(obs_table))\
							.filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)\
							.group_by(obs_table.polarization, obs_table.era_type).all()
		for polarization, era_type, length, count in response:
			obs_map[polarization][str(era_type)].update({'obs_count': count , 'obs_hours': length})

		file_table = pdbi.File
		response = s.query(file_table.host, file_table.filetype, func.count(file_table)).group_by(file_table.host, file_table.filetype).all()
		for host, filetype, count in response:
			file_map[host][filetype].update({'file_count': count})

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

	#gets rid of useless columns
	no_obs = {era_type: {'obs_count': 0, 'obs_hours': 0} for era_type in era_type_strs}
	pol_strs = tuple(pol for pol, era_type_dict in obs_map.items() if era_type_dict != no_obs)

	no_files = {filetype: {'file_count': 0} for filetype in filetype_strs}
	host_strs = tuple(host for host, filetype_dict in file_map.items() if filetype_dict != no_files)

	return render_template('summary_table.html',
							pol_strs=pol_strs, era_type_strs=era_type_strs, host_strs=host_strs, filetype_strs=filetype_strs,
							obs_map=obs_map, obs_total=obs_total, file_map=file_map, file_total=file_total)

@app.route('/day_summary_table', methods=['POST'])
def day_summary_table():
	'''
	summary of data in main databases

	Returns
	-------
	html: day summary table
	'''
	starttime = request.form['starttime']
	endtime = request.form['endtime']

	jdstart = request.form['jd_start']
	jdend = request.form['jd_end']

	start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		obs_table = pdbi.Observation
		response = s.query(obs_table.julian_day, func.count(obs_table))\
							.filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)\
							.group_by(obs_table.julian_day).order_by(obs_table.julian_day.asc()).all()
		day_map = tuple((julian_day, count) for julian_day, count in response)

	return render_template('day_summary_table.html', day_map=day_map)
