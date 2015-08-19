from flask import render_template, flash, redirect, url_for, request, g, make_response
from flask.ext.login import current_user
from app.flask_app import app, db
from app import models, db_utils, histogram_utils, data_sources
from datetime import datetime
import os
import paperdata_dbi as pdbi
import pyganglia as pyg

@app.route('/')
@app.route('/index')
@app.route('/index/set/<setName>')
@app.route('/set/<setName>')
def index(setName = None):
	active_data_sources = []

	if g.user is not None and g.user.is_authenticated():
		active_data_sources = g.user.active_data_sources

	if setName is not None:
		the_set = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
														field_tuples=(('name', '==', setName),),
														sort_tuples=None, output_vars=None)[0]

		if the_set is not None:
			start_datetime, end_datetime = db_utils.get_datetime_from_gps(
				the_set.start, the_set.end)
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

	data_source = db_utils.get_query_results(data_source=None, database='eorlive', table='graph_data_source',
													field_tuples=(('name', '==', data_source_str),),
													sort_tuples=None, output_vars=None)[0]

	set_str = request.args.get('set')

	template_name = ''.join(('js/', graph_type_str.lower(), '.js'))

	if set_str is None: # There should be a date range instead.
		start_time_str = request.args.get('start')
		end_time_str = request.args.get('end')
		if start_time_str is None or end_time_str is None:
			return make_response('No date range specified', 500)

		start_datetime = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%SZ')

		end_datetime = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ')

		start_gps, end_gps = db_utils.get_gps_from_datetime(start_datetime, end_datetime)

		if graph_type_str == 'Obs_Err':
			return histogram_utils.get_obs_err_histogram(start_gps, end_gps,
				start_time_str, end_time_str)
		else:
			graph_data = data_sources.get_graph_data(data_source_str, start_gps, end_gps, None)
			data_source_str_nospace = data_source_str.replace(' ', 'ಠ_ಠ')
			return render_template('graph.html',
				data_source_str=data_source_str, graph_data=graph_data,
				plot_bands=[], template_name=template_name, is_set=False,
				data_source_str_nospace=data_source_str_nospace,
				start_time_str_short=start_datetime.strftime('%Y-%m-%d %H:%M'),
				end_time_str_short=end_datetime.strftime('%Y-%m-%d %H:%M'),
				width_slider=data_source.width_slider)
	else:
		the_set = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
														field_tuples=(('name', '==', set_str),),
														sort_tuples=None, output_vars=None)[0]

		if the_set is None:
			return make_response('Set not found', 500)

		plot_bands = histogram_utils.get_plot_bands(the_set)

		start_datetime, end_datetime = db_utils.get_datetime_from_gps(
				the_set.start, the_set.end)

		start_time_str_short = start_datetime.strftime('%Y-%m-%d %H:%M')
		end_time_str_short = end_datetime.strftime('%Y-%m-%d %H:%M')

		if graph_type_str == 'Obs_Err':
			observation_counts, utc_obsid_map = histogram_utils.get_observation_counts(
				the_set.start, the_set.end, the_set.low_or_high, the_set.eor)
			error_counts = histogram_utils.get_error_counts(the_set.start, the_set.end)[0]
			range_end = end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') # For the function in histogram_utils.js
			which_data_set = data_sources.which_data_set(the_set)
			return render_template('setView.html', the_set=the_set,
				observation_counts=observation_counts, error_counts=error_counts,
				plot_bands=plot_bands, start_time_str_short=start_time_str_short,
				end_time_str_short=end_time_str_short, range_end=range_end,
				which_data_set=which_data_set, is_set=True,
				utc_obsid_map=utc_obsid_map)
		else:
			graph_data = data_sources.get_graph_data(data_source_str, the_set.start, the_set.end, the_set)
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
	data = db_utils.get_query_results(data_source=None, database='eorlive', table='data_amount',
										field_tuples=None,
										sort_tuples=(('created_on', 'desc'),), output_vars=None)[0]


	data_time = hours_sadb = hours_paperdata = hours_with_data = 'N/A'

	if data is not None:
		data = data.to_json()
		data_time = data['created_on']
		hours_sadb = data['hours_sadb']
		hours_paperdata = data['hours_paperdata']
		hours_with_data = data['hours_with_data']

	return render_template('data_amount_table.html', hours_sadb=hours_sadb, hours_paperdata=hours_paperdata,
							hours_with_data=hours_with_data, data_time=data_time)

@app.route('/source_table', methods = ['GET'])
def source_table():
	data = db_utils.get_query_results(data_source=None, database='paperdata', table='rtp_file',
										field_tuples=None,
										sort_tuples=(('timestamp', 'desc'),), output_vars=None)


	data_time = hours_sadb = hours_paperdata = hours_with_data = 'N/A'

	if data is not None:
		data = data.to_json()
		data_time = data['created_on']
		hours_sadb = data['hours_sadb']
		hours_paperdata = data['hours_paperdata']
		hours_with_data = data['hours_with_data']

	return render_template('data_amount_table.html', hours_sadb=hours_sadb, hours_paperdata=hours_paperdata,
							hours_with_data=hours_with_data, data_time=data_time)

@app.route('/error_table', methods = ['POST'])
def error_table():
	starttime = datetime.utcfromtimestamp(int(request.form['starttime']) / 1000)

	endtime = datetime.utcfromtimestamp(int(request.form['endtime']) / 1000)

	start_gps, end_gps = db_utils.get_gps_from_datetime(starttime, endtime)

	obscontroller_response = db_utils.get_query_results(data_source=None, database='eor', table='obscontroller_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time', 'observation_number', 'comment'))

	recvstatuspolice_response = db_utils.get_query_results(data_source=None, database='eor', table='recvstatuspolice_log',
														(('reference_time', '>=', start_gps), ('reference_time', '<=', end_gps)),
														sort_tuples=(('reference_time', 'asc'),),
														output_vars=('reference_time', 'observation_number', 'comment'))

	return render_template('error_table.html', obscontroller_error_list=obscontroller_response,
							recvstatuspolice_error_list=recvstatuspolice_response,
							start_time=starttime.strftime('%Y-%m-%dT%H:%M:%SZ'),
							end_time=endtime.strftime('%Y-%m-%dT%H:%M:%SZ'))

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
		user = db_utils.get_query_results(data_source=None, database='eorlive', table='user',
											field_tuples=(('username', '==', g.user.username),), sort_tuples=None, output_vars=None)[0]

		setList = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
														field_tuples=(('username', '==', g.user.username),),
														sort_tuples=None, output_vars=None)[0]

		return render_template('profile.html', user=user, sets=setList)
	else:
		return redirect(url_for('login'))

@app.route('/user_page')
def user_page():
	if (g.user is not None and g.user.is_authenticated()):
		user = db_utils.get_query_results(data_source=None, database='eorlive', table='user',
											field_tuples=(('username', '==', g.user.username),), sort_tuples=None, output_vars=None)[0]

		userList = db_utils.get_query_results(data_source=None, database='eorlive', table='user',
											field_tuples=None, sort_tuples=None, output_vars=None)[0]

		setList = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
														field_tuples=None, sort_tuples=None, output_vars=None)[0]

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

	start_gps, end_gps = db_utils.get_gps_from_datetime(startdatetime, enddatetime)

	response = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
										(('starttime', '>=', start_gps), ('starttime', '<=', end_gps),
										sort_tuples=(('starttime', 'asc'),),
										output_vars=('starttime', 'stoptime', 'obsname', 'ra_phase_center'))

	low_eor0_count = low_eor1_count = high_eor0_count = high_eor1_count = 0
	low_eor0_hours = low_eor1_hours = high_eor0_hours = high_eor1_hours = 0

	for observation in response:
		start_time = observation[0]
		stop_time = observation[1]
		obs_name = observation[2]

		try:
			ra_phase_center = int(observation[3])
		except TypeError as te:
			ra_phase_center = -1

		data_hours = (stop_time - start_time) / 3600

		if 'low' in obs_name:
			if ra_phase_center == 0:
				low_eor0_count += 1
				low_eor0_hours += data_hours
			elif ra_phase_center == 60:
				low_eor1_count += 1
				low_eor1_hours += data_hours
		elif 'high' in obs_name:
			if ra_phase_center == 0:
				high_eor0_count += 1
				high_eor0_hours += data_hours
			elif ra_phase_center == 60:
				high_eor1_count += 1
				high_eor1_hours += data_hours

	error_counts, error_count = histogram_utils.get_error_counts(start_gps, end_gps)

	return render_template('summary_table.html', error_count=error_count,
		low_eor0_count=low_eor0_count, high_eor0_count=high_eor0_count,
		low_eor1_count=low_eor1_count, high_eor1_count=high_eor1_count,
		low_eor0_hours=low_eor0_hours, high_eor0_hours=high_eor0_hours,
		low_eor1_hours=low_eor1_hours, high_eor1_hours=high_eor1_hours)
