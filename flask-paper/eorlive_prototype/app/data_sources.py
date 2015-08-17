from flask import render_template, request, g, make_response, jsonify
import psycopg2
import os
import re
from app.flask_app import app, db
from app import db_utils, models
#import paperdata_dbi as pdbi
#import pyganglia_dbi as pyg
#from sqlalchemy.engine import reflection
#from sqlalchemy import or_

#def get_dbi(database):
#	if database == 'paperdata':
#		dbi = pdbi.DataBaseInterface()
#	elif database == 'ganglia':
#		dbi = pyg.DataBaseInterface()
#	return dbi, module

#def inspector(database):
#	dbi, _ = get_dbi(database)
#	insp = reflection.Inspector.from_engine(dbi.engine)
#	return insp

@app.route('/get_tables', methods = ['POST'])
def get_tables():
	if g.user is not None and g.user.is_authenticated():
		hostname = request.form['hostname']
		database = request.form['database']

		try:
			db_conn = psycopg2.connect(database=database, host=hostname,
				user=os.environ['MWA_DB_USERNAME'], password=os.environ['MWA_DB_PW'])
		except Exception as e:
			return 'Cannot connect to database'

		table_tuples = db_utils.send_query(db_conn, '''SELECT table_name
													FROM information_schema.tables
													WHERE table_schema='public'
													ORDER BY table_name''').fetchall()

		db_conn.close()

		#insp = inspector(database)
		#table_tuples = insp.get_table_names()

		return render_template('table_list.html', table_tuples=table_tuples)
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/get_columns', methods = ['POST'])
def get_columns():
	if g.user is not None and g.user.is_authenticated():
		hostname = request.form['hostname']
		database = request.form['database']
		table = request.form['table']

		try:
			db_conn = psycopg2.connect(database=database, host=hostname,
				user=os.environ['MWA_DB_USERNAME'], password=os.environ['MWA_DB_PW'])
		except Exception as e:
			return 'Cannot connect to database'

		column_tuples = db_utils.send_query(db_conn, '''SELECT column_name
													FROM information_schema.columns
													WHERE table_name = '{table}'
													AND numeric_precision IS NOT NULL'''.format(table=table)).fetchall()

		db_conn.close()

		#insp = inspector(database)
		#it's a list of dicts
		#column_list = insp.get_column_names(table)
		#column_tuples = tuple(column['name'] for column in column_list)

		return render_template('column_list.html', column_tuples=column_tuples)
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/get_users_data_sources')
def get_users_data_sources():
	if g.user is not None and g.user.is_authenticated():
		active_data_sources = g.user.active_data_sources
		subscribed_but_inactive_data_sources =\
			list(set(g.user.subscribed_data_sources) - set(active_data_sources))

		return render_template('data_sources.html',
			subscribed_but_inactive_data_sources=subscribed_but_inactive_data_sources,
			active_data_sources=g.user.active_data_sources)
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/get_unsubscribed_data_sources')
def get_unsubscribed_data_sources():
	if g.user is not None and g.user.is_authenticated():
		all_data_sources = models.GraphDataSource.query.all()
		subscribed_data_sources = g.user.subscribed_data_sources

		unsubscribed_data_sources = list(set(all_data_sources) -\
			set(subscribed_data_sources))

		return render_template('unsubscribed_data_sources.html',
			unsubscribed_data_sources=unsubscribed_data_sources)
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/update_active_data_sources', methods = ['POST'])
def update_active_data_sources():
	if g.user is not None and g.user.is_authenticated():
		request_content = request.get_json()
		new_active_data_sources_names = request_content['activeDataSources']

		new_active_data_sources = models.GraphDataSource.query.filter(
			models.GraphDataSource.name.in_(new_active_data_sources_names)).all()
		current_active_data_sources = g.user.active_data_sources
		active_to_remove = list(set(current_active_data_sources) -\
			set(new_active_data_sources))
		active_to_add = list(set(new_active_data_sources) -\
			set(current_active_data_sources))

		for active_data_source in active_to_remove:
			g.user.active_data_sources.remove(active_data_source)

		for active_data_source in active_to_add:
			g.user.active_data_sources.append(active_data_source)

		db.session.add(g.user)
		db.session.commit()
		return 'Success'
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/subscribe_to_data_source', methods = ['POST'])
def subscribe_to_data_source():
	if g.user is not None and g.user.is_authenticated():
		data_source_name = request.form['dataSource']

		data_source = models.GraphDataSource.query.filter(
			models.GraphDataSource.name == data_source_name).first()

		g.user.subscribed_data_sources.append(data_source)
		db.session.add(g.user)
		db.session.commit()
		return 'Success'
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/unsubscribe_from_data_source', methods = ['POST'])
def unsubscribe_from_data_source():
	if g.user is not None and g.user.is_authenticated():
		data_source_name = request.form['dataSource']

		data_source = models.GraphDataSource.query.filter(
			models.GraphDataSource.name == data_source_name).first()

		g.user.subscribed_data_sources.remove(data_source)
		try:
			g.user.active_data_sources.remove(data_source)
		except ValueError as e:
			#The user didn't have this as an active data source.
			print('Tried to remove an inactive data source')
		db.session.add(g.user)
		db.session.commit()
		return 'Success'
	else:
		return make_response('You must be logged in to use this feature.', 401)

@app.route('/get_graph_types')
def get_graph_types():
	graph_types = models.GraphType.query.filter(models.GraphType.name != 'Obs_Err').all()
	return render_template('graph_type_list.html', graph_types=graph_types)

@app.route('/create_data_source', methods = ['POST'])
def create_data_source():
	if g.user is not None and g.user.is_authenticated():
		request_content = request.get_json()

		graph_type = request_content['graph_type']
		host = request_content['host']
		database = request_content['database']
		table = request_content['table']
		columns = request_content['columns']
		obs_column = request_content['obs_column']
		data_source_name = request_content['data_source_name']
		include_width_slider = request_content['include_width_slider']

		if not graph_type or not host or not database or not table or not columns\
			or not obs_column or not data_source_name:
			return jsonify(error=True, message='You need to fill out all the fields.')

		data_source_name = data_source_name.strip()
		if len(data_source_name) == 0:
			return jsonify(error=True, message='Name cannot be empty.')

		# The data source name (with spaces replaced by ಠ_ಠ) is used as
		# a JavaScript variable name and as an ID in HTML, so it needs
		# to obey the rules for those identifiers, minus a few
		# options such as $ since they would need to be escaped in
		# the HTML IDs.
		if not re.match(r'^[a-zA-Z_][0-9a-zA-Z_ ]*$', data_source_name):
			return jsonify(error=True, message='''Your data source name must
				start with a letter or _ that is followed by digits,
				letters, _, or spaces.''')

		#Is the data source name unique?
		if models.GraphDataSource.query.filter(models.GraphDataSource.name == data_source_name).first() is not None:
			return jsonify(error=True, message='The data source name must be unique.')

		try:
			db_conn = psycopg2.connect(database=database, host=host,
				user=os.environ['MWA_DB_USERNAME'], password=os.environ['MWA_DB_PW'])
		except Exception as e:
			return jsonify(error=True, message='Could not connect to that database.')

		table_response = db_utils.send_query(db_conn, '''SELECT table_name
											FROM information_schema.tables
											WHERE table_name = '{table}'
											AND table_schema='public''''.format(table=table)).fetchall()

		if len(table_response) == 0: #No results, so the table does not exist.
			db_conn.close()
			return jsonify(error=True, message='The table {table} does not exist.'.format(table=table))

		#insp = inspector(database)
		#table_tuples = insp.get_table_names()
		#if table not in table_tuples:
		#	return jsonify(error=True, message='The table {table} does not exist.'.format(table=table))

		column_response = db_utils.send_query(db_conn, '''SELECT column_name
											FROM information_schema.columns
											WHERE table_name = '{table}'
											AND numeric_precision IS NOT NULL'''.format(table=table)).fetchall()

		db_conn.close()

		columns_with_obs_column = list(columns)
		columns_with_obs_column.append(obs_column)

		#it's a list of dicts
		#column_list = insp.get_column_names(table)
		#column_response = tuple(column['name'] for column in column_list)

		for column in columns_with_obs_column: # So we can check for the existence of the obs column along with the
			column_exists = False			  # others in the same loop.
			for returned_column in column_response:
				if returned_column[0] == column:
					column_exists = True
					break
			if not column_exists:
				return jsonify(error=True,
					message='The column {column} does not exist in that table or is not a numeric column.'.format(column=column))

		#missing_columns = tuple(column if column not in column_reponse for columns_with_obs_column)
		#if missing_columns:
		#	return jsonify(error=True,
		#		message='The column {column} does not exist in that table or is not a numeric column.'.format(column=missing_columns[0]))
		
		projectid = False
		for returned_column in column_response:
			if returned_column[0] == 'projectid':
				projectid = True
				break

		#projectid = 'projectid' in column_response

		graph_data_source = models.GraphDataSource()
		graph_data_source.name = data_source_name
		graph_data_source.graph_type = graph_type
		graph_data_source.host = host
		graph_data_source.database = database
		graph_data_source.table = table
		graph_data_source.obs_column = obs_column
		graph_data_source.projectid = projectid
		graph_data_source.width_slider = include_width_slider
		db.session.add(graph_data_source)
		db.session.flush()

		for column in columns:
			graph_data_source_column = models.GraphDataSourceColumn()
			graph_data_source_column.name = column
			graph_data_source_column.graph_data_source = data_source_name
			db.session.add(graph_data_source_column)

		g.user.subscribed_data_sources.append(graph_data_source)
		db.session.add(g.user)

		db.session.commit()

		return jsonify(error=False)
	else:
		return make_response('You must be logged in to use this feature.', 401)

def build_query(data_source):
	query = ''.join(('SELECT ', data_source.obs_column))
	columns = models.GraphDataSourceColumn.query.filter(
		models.GraphDataSourceColumn.graph_data_source == data_source.name).all()

	for column in columns:
		query = ', '.join((query, column.name))

	query = ''.join((query, ' FROM ', data_source.table, ' WHERE ', data_source.obs_column, ' >= {} AND ', data_source.obs_column, ' <= {} ',
						''' AND projectid='G0009' ''' if data_source.projectid else '', ' ORDER BY ', data_source.obs_column, ' ASC'))

	return (query, columns)

##def make_clause(table, field_name, equivalency, value):
##	field = getattr(table, field_name)
##	if equivalency == '<='
##		clause = field <= value
##	elif equivalency == '=='
##		clause = field == value
##	elif equivalency == '>='
##		clause = field >= value
##	elif equivalency == 'like'
##		clause = field.like(value)
##	elif equivalency == 'or':
##		clause_tuple = tuple(make_clause(table, new_field_name, new_equivalency, new_value)
##										for new_field_name, new_equivalency, new_value in field_name)
##		clause = or_(*clause_tuple)
##	return clause

##def gen_results(table, results, field_name, equivalency, value):
##	field = getattr(table, field_name)
##	if equivalency == '<='
##		results = results.filter(field <= value)
##	elif equivalency == '=='
##		results = results.filter(field == value)
##	elif equivalency == '>='
##		results = results.filter(field >= value)
##	elif equivalency == 'like'
##		results = results.filter(field.like(value))
##	return results

##def get_query_results(data_source, field_tuples, field_sort_name, output_vars=None):
##	#field tuples is list of field tuples containting field_name, equivalency, value and in that order
##	#ex: [('obs_column', '<=', 23232), ('projectid', '==', 'G0009')]
##	dbi, module = get_dbi(data_source.database)
##	s = dbi.Session()
##	table = getattr(module, data_source.table.capitalize())
##	results = s.query(table)
##	for field_name, equivalency, value in field_tuples:
##		if equivalency == 'or' and value is None:
##			for new_field_name, new_equivalency, new value in field_name:
##				results = gen_results(table, results, new_field_name, new_equivalency, new_value)
##		else:
##			results = gen_results(table, results, field_name, equivalency, value)
##	field_sort = getattr(table, field_sort_name)
##	results = results.order_by(field_sort.asc()).all()
##	if output_vars is not None:
##		results = tuple((getattr(entry, output_var) for output_var in output_vars) for entry in results))
##	s.close()
##	return results

##def get_query_results(data_source, field_tuples, field_sort_name, output_vars=None):
##	#field tuples is list of field tuples containting field_name, equivalency, value and in that order
##	#ex: [('obs_column', '<=', 23232), ('projectid', '==', 'G0009')]
##	dbi, module = get_dbi(data_source.database)
##	s = dbi.Session()
##	table = getattr(module, data_source.table.capitalize())
##	results = s.query(table)
##	#tuple of all results
##	results_tuples = (results,)
##	for field_name, equivalency, value in field_tuples:
##	
##		#list purged on every loop
##		results_list = []
##		if equivalency == 'or' and value is None:
##			#run through each set
##			for new_results in results_tuples:
##				for new_field_name, new_equivalency, new value in field_name:
##					results_list.append(gen_results(table, new_results, new_field_name, new_equivalency, new_value))
##		else:
##			for new_results in results_tuples:
##				results_list.append(gen_results(table, new_results, field_name, equivalency, value))
##		results_tuples = tuple(results_list)
##	#now should have couple of sets
##	field_sort = getattr(table, field_sort_name)
##	flatten and make a set to be unique
##	results_list = tuple(results.order_by(field_sort.asc()).all() for results in results_tuples)
##	#grab relevant variables
##	if output_vars is None:
##		results = tuple({tuple(getattr(entry, output_var) for output_var in output_vars) for result in results_list for entry in result})
##	else:
##		results = tuple({entry for result in results_list for entry in result})
##	s.close()
##	return results

def get_graph_data(data_source_str, start_gps, end_gps, the_set):
	data_source = models.GraphDataSource.query.filter(models.GraphDataSource.name == data_source_str).first()

	query, columns = build_query(data_source)

	db_conn = psycopg2.connect(database=data_source.database, host=data_source.host,
		user=os.environ['MWA_DB_USERNAME'], password=os.environ['MWA_DB_PW'])

	results = db_utils.send_query(db_conn, query.format(start_gps, end_gps)).fetchall()

	db_conn.close()

	##results = get_query_results(data_source, (('obs_column, '>=', start_gps), ('obs_column', '<=', end_gps),
	##			('projectid', '==', 'G0009' if data_source.projectid else '')), field_sort_name='obs_column')

	data = {}

	if the_set is not None:
		# Initialize empty data structure
		data['series_dict'] = {} # 'series_dict' holds the data for each series (table column) in the data source
		for column in columns:
			data['series_dict'][column.name] = []

		GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

		utc_obsid_map = []
		results = join_with_obsids_from_set(results, the_set, data_source)

		for row in results:
								# Actual UTC time of the observation (for the graph)
			utc_millis = int((row[0] - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000)
			for column_index in range(1, len(row)):
				data['series_dict'][columns[column_index - 1].name].append([utc_millis, row[column_index]])
			utc_obsid_map.append([utc_millis, row[0]])

		# l0, l1, h0, h1, or 'any'
		whichDataSet = which_data_set(the_set)
		data[whichDataSet] = utc_obsid_map
	else: #No set, so we need to separate the data into sets for low/high and EOR0/EOR1
		data = separate_data_into_sets(data, results, columns, data_source, start_gps, end_gps)

	return data

def which_data_set(the_set):
	is_any = the_set.low_or_high == 'any' or the_set.eor == 'any'
	return 'any' if is_any else the_set.low_or_high[0] + the_set.eor[3]

def separate_data_into_sets(data, data_source_results, columns, data_source, start_gps, end_gps):
	projectid_clause = '''AND projectid='G0009'''' if data_source.projectid else ''
	obsid_results = db_utils.send_query(g.eor_db, '''SELECT starttime, obsname, ra_phase_center
									FROM mwa_setting
									WHERE starttime >= {start} AND starttime <= {end}
									{projectid}
									AND (obsname LIKE 'low%' OR obsname LIKE 'high%')
									AND (ra_phase_center = 0 OR ra_phase_center = 60)
									ORDER BY starttime ASC'''.format(start=start_gps,
									end=end_gps, projectid=projectid_clause)).fetchall()

	#sqlalchemy overloads the |, &, and ~ operators, can use as or, and, not
	##obsid_results = get_query_results(data_source, (('starttime, '>=', start_gps), ('starttime', '<=', end_gps),
	##									('projectid', '==', 'G0009' if data_source.projectid else ''),
	##									((('obsname', 'like', 'low%'), ('obsname', 'like', 'high%')), 'or', None)),
	##									((('ra_phase_center', '==', 0), ('ra_phase_center', '==', 60)), 'or', None),
	##									field_sort_name='starttime', output_vars=('starttime', 'obsname', 'ra_phase_center'))

	data['l0'] = {}
	data['l1'] = {}
	data['h0'] = {}
	data['h1'] = {}

	for key in data:
		for column in columns:
			data[key][column.name] = []

	data['utc_obsid_map_l0'] = []
	data['utc_obsid_map_l1'] = []
	data['utc_obsid_map_h0'] = []
	data['utc_obsid_map_h1'] = []

	all_obsids = [obsid_tuple[0] for obsid_tuple in obsid_results]

	GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

	for data_source_result_tuple in data_source_results:
		obsid = data_source_result_tuple[0]

		try:
			obsid_index = all_obsids.index(obsid)
		except Exception as e:
			continue

		utc_millis = (obsid - GPS_LEAP_SECONDS_OFFSET + GPS_UTC_DELTA) * 1000

		obsname = obsid_results[obsid_index][1]
		ra_phase_center = obsid_results[obsid_index][2]

		if obsname.startswith('low') and ra_phase_center == 0:
			for column_index in range(1, len(data_source_result_tuple)):
				column_name = columns[column_index - 1].name
				column_data = data_source_result_tuple[column_index]
				data['l0'][column_name].append([utc_millis, column_data])
			data['utc_obsid_map_l0'].append([utc_millis, obsid])
		elif obsname.startswith('low') and ra_phase_center == 60:
			for column_index in range(1, len(data_source_result_tuple)):
				column_name = columns[column_index - 1].name
				column_data = data_source_result_tuple[column_index]
				data['l1'][column_name].append([utc_millis, column_data])
			data['utc_obsid_map_l1'].append([utc_millis, obsid])
		elif obsname.startswith('high') and ra_phase_center == 0:
			for column_index in range(1, len(data_source_result_tuple)):
				column_name = columns[column_index - 1].name
				column_data = data_source_result_tuple[column_index]
				data['h0'][column_name].append([utc_millis, column_data])
			data['utc_obsid_map_h0'].append([utc_millis, obsid])
		elif obsname.startswith('high') and ra_phase_center == 60:
			for column_index in range(1, len(data_source_result_tuple)):
				column_name = columns[column_index - 1].name
				column_data = data_source_result_tuple[column_index]
				data['h1'][column_name].append([utc_millis, column_data])
			data['utc_obsid_map_h1'].append([utc_millis, obsid])

	return data

def join_with_obsids_from_set(data_source_results, the_set, data_source):
	low_high_clause, eor_clause = db_utils.get_lowhigh_and_eor_clauses(the_set.low_or_high, the_set.eor)

	projectid_clause = '''AND projectid='G0009'''' if data_source.projectid else ''

	response = db_utils.send_query(g.eor_db, '''SELECT starttime
				FROM mwa_setting
				WHERE starttime >= {start} AND starttime <= {end}
				{projectid}
				{low_high}
				{eor}
				ORDER BY starttime ASC'''.format(start=the_set.start, end=the_set.end,
					projectid=projectid_clause, low_high=low_high_clause, eor=eor_clause)).fetchall()

	##response = get_query_results(data_source, (('starttime, '>=', the_set.start), ('starttime', '<=', the_set.end),
	##									('projectid', '==', 'G0009' if data_source.projectid else ''),
	##									((('obsname', 'like', 'low%'), ('obsname', 'like', 'high%')), 'or', None)),
	##									((('ra_phase_center', '==', 0), ('ra_phase_center', '==', 60)), 'or', None),
	##									field_sort_name='starttime', output_vars=('starttime', 'obsname', 'ra_phase_center'))

	obs_id_list = [obs_tuple[0] for obs_tuple in response]

	filtered_results = [obs_tuple for obs_tuple in data_source_results if obs_tuple[0] in obs_id_list]

	return filtered_results
