from app import db_utils, models
from app.flask_app import app, db
from flask import request, g, make_response, jsonify, render_template
from datetime import datetime

def insert_set_into_db(name, start, end, flagged_range_dicts, low_or_high,
		eor, total_data_hrs, flagged_data_hrs):
	new_set = models.Set()
	new_set.username = g.user.username
	new_set.name = name
	new_set.start = start
	new_set.end = end
	new_set.low_or_high = low_or_high
	new_set.eor = eor
	new_set.total_data_hrs = total_data_hrs
	new_set.flagged_data_hrs = flagged_data_hrs

	##new_set = getattr(models, 'Set')()
	##setattr(new_set, 'username', g.user.username)
	##setattr(new_set, 'name', name)
	##setattr(new_set, 'start', start)
	##setattr(new_set, 'end', end)
	##setattr(new_set, 'low_or_high', low_or_high)
	##setattr(new_set, 'eor', eor)
	##setattr(new_set, 'total_data_hrs', total_data_hrs)
	##setattr(new_set, 'flagged_data_hrs', flagged_data_hrs)

	db.session.add(new_set)
	db.session.flush()
	db.session.refresh(new_set) # So we can get the set's id

	for flagged_range_dict in flagged_range_dicts:
		flagged_subset = models.Flagged_Subset()
		flagged_subset.set_id = new_set.id
		flagged_subset.start = flagged_range_dict['start_gps']
		flagged_subset.end = flagged_range_dict['end_gps']

		##flagged_subset = getattr(models, 'Flagged_Subset')()
		##setattr(flagged_subset, 'set_id', getattr(new_set, 'id'))
		##setattr(flagged_subset, 'start', flagged_range_dict['start_gps'])
		##setattr(flagged_subset, 'end', flagged_range_dict['end_gps'])

		db.session.add(flagged_subset)
		db.session.flush()
		db.session.refresh(flagged_subset) # So we can get the id

		for obs_id in flagged_range_dict['flaggedRange']:
			flagged_obs_id = models.Flagged_Obs_Ids()
			flagged_obs_id.obs_id = obs_id
			flagged_obs_id.flagged_subset_id = flagged_subset.id

			##flagged_obs_id = getattr(models, 'Flagged_Obs_Ids')()
			##setattr(flagged_obs_id, 'obs_id', obs_id)
			##setattr(flagged_obs_id, 'flagged_subset_id', getattr(flagged_subset, 'id'))

			db.session.add(flagged_obs_id)

	db.session.commit()

def is_obs_flagged(obs_id, flagged_range_dicts):
	for flagged_range_dict in flagged_range_dicts:
		if obs_id >= flagged_range_dict['start_gps'] and obs_id <= flagged_range_dict['end_gps']:
			return True
	return False

def get_data_hours_in_set(start, end, low_or_high, eor, flagged_range_dicts):
	total_data_hrs = flagged_data_hrs = 0

	low_high_clause, eor_clause = db_utils.get_lowhigh_and_eor_clauses(low_or_high, eor)

	all_obs_ids_tuples = db_utils.send_query(g.eor_db, '''SELECT starttime, stoptime
							FROM mwa_setting
							WHERE starttime >= {start} AND starttime <= {end}
							AND projectid='G0009'
							{low_high}
							{eor}
							ORDER BY starttime ASC'''.format(start=start, end=end, low_high=low_high_clause, eor=eor_clause)).fetchall()

	##all_obs_ids_tuples = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
	##									(('starttime', '>=', start), ('starttime', '<=', end),
	##									('projectid', '==', 'G0009'),
	##									('obsname', None if low_or_high == 'any' else 'like', ''.join(low_or_high, '%')),
	##									('ra_phase_center', None if eor == 'any' else '==', 0 if eor == 'EOR0' else 60))
	##									field_sort_tuple=(('starttime', 'asc'),), output_vars=('starttime', 'stoptime'))

	for obs in all_obs_ids_tuples:
		obs_id = obs[0]
		data_hrs = (obs[1] - obs_id) / 3600
		total_data_hrs += data_hrs
		if is_obs_flagged(obs_id, flagged_range_dicts):
			flagged_data_hrs += data_hrs

	return (total_data_hrs, flagged_data_hrs)

@app.route('/save_new_set', methods=['POST'])
def save_new_set():
	if (g.user is not None and g.user.is_authenticated()):
		request_content = request.get_json()

		name = request_content['name']

		if name is None:
			return jsonify(error=True, message='Name cannot be empty.')

		name = name.strip()

		if len(name) == 0:
			return jsonify(error=True, message='Name cannot be empty.')

		if models.Set.query.filter(models.Set.name == name).count() > 0:
			return jsonify(error=True, message='Name must be unique.')

		flagged_range_dicts = []

		GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA = db_utils.get_gps_utc_constants()

		for flagged_range_dict in request_content['flaggedRanges']:
			flagged_gps_dict = {}
			flagged_gps_dict['flaggedRange'] = [pair[1] for pair in flagged_range_dict['flaggedRange']]
			flagged_gps_dict['start_gps'] = int(flagged_range_dict['start_millis'] / 1000) +\
				GPS_LEAP_SECONDS_OFFSET - GPS_UTC_DELTA
			flagged_gps_dict['end_gps'] = int(flagged_range_dict['end_millis'] / 1000) +\
				GPS_LEAP_SECONDS_OFFSET - GPS_UTC_DELTA
			flagged_range_dicts.append(flagged_gps_dict)

		start_gps = request_content['startObsId']
		end_gps = request_content['endObsId']
		low_or_high = request_content['lowOrHigh']
		eor = request_content['eor']

		total_data_hrs, flagged_data_hrs = get_data_hours_in_set(
			start_gps, end_gps, low_or_high, eor, flagged_range_dicts)

		insert_set_into_db(name, start_gps, end_gps, flagged_range_dicts,
			low_or_high, eor, total_data_hrs, flagged_data_hrs)

		return jsonify()
	else:
		return make_response('You need to be logged in to save a set.', 401)

@app.route('/upload_set', methods=['POST'])
def upload_set():
	if (g.user is not None and g.user.is_authenticated()):
		set_name = request.form['set_name']

		if set_name is None:
			return jsonify(error=True, message='Name cannot be empty.')

		set_name = set_name.strip()

		if len(set_name) == 0:
			return jsonify(error=True, message='Name cannot be empty.')

		if models.Set.query.filter(models.Set.name == set_name).count() > 0:
			return jsonify(error=True, message='Name must be unique.')

		f = request.files['file']

		good_obs_ids = []

		for line in f.stream:
			line = str(line.decode('utf-8').strip())
			if line == '':
				continue
			try:
				obs_id = int(line)
				good_obs_ids.append(obs_id)
			except ValueError as ve:
				return jsonify(error=True, message=''.join('Invalid content in file: ', line))

		good_obs_ids.sort()

		start_gps = good_obs_ids[0]
		end_gps = good_obs_ids[len(good_obs_ids) - 1]

		low_or_high = request.form['low_or_high']
		eor = request.form['eor']

		low_high_clause, eor_clause = db_utils\
			.get_lowhigh_and_eor_clauses(low_or_high, eor)

		all_obs_ids_tuples = db_utils.send_query(g.eor_db, '''SELECT starttime
							FROM mwa_setting
							WHERE starttime >= {start} AND starttime <= {end}
							AND projectid='G0009'
							{low_high}
							{eor}
							ORDER BY starttime ASC'''.format(start=start_gps, end=end_gps, low_high=low_high_clause, eor=eor_clause)).fetchall()

		##all_obs_ids_tuples = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
		##									(('starttime', '>=', start_gps), ('starttime', '<=', end_gps),
		##									('projectid', '==', 'G0009'),
		##									('obsname', None if low_or_high == 'any' else 'like', ''.join(low_or_high, '%')),
		##									('ra_phase_center', None if eor == 'any' else '==', 0 if eor == 'EOR0' else 60))
		##									field_sort_tuple=(('starttime', 'asc'),), output_vars=('starttime',))

		all_obs_ids = [tup[0] for tup in all_obs_ids_tuples]

		last_index = 0

		bad_ranges = []

		for good_obs_id in good_obs_ids:
			try:
				next_index = all_obs_ids.index(good_obs_id)
			except ValueError as e:
				return jsonify(error=True, message=''.join('Obs ID ', str(good_obs_id),
					''' not found in the set of observations corresponding
						to Low/High: {low_high} and EOR: {eor}'''.format(low_high=low_or_high, eor=eor)))
			if next_index > last_index:
				bad_range_dict = {}
				bad_range_dict['start_gps'] = all_obs_ids[last_index]
				bad_range_dict['end_gps'] = all_obs_ids[next_index - 1]
				bad_range_dict['flaggedRange'] = all_obs_ids[last_index:next_index]
				bad_ranges.append(bad_range_dict)

			last_index = next_index + 1

		total_data_hrs, flagged_data_hrs = get_data_hours_in_set(
			start_gps, end_gps, low_or_high, eor, bad_ranges)

		insert_set_into_db(set_name, start_gps, end_gps, bad_ranges,
			low_or_high, eor, total_data_hrs, flagged_data_hrs)

		return 'OK'
	else:
		return make_response('You need to be logged in to upload a set.', 401)

@app.route('/download_set')
def download_set():
	set_id = request.args['set_id']

	the_set = models.Set.query.filter(models.Set.id == set_id).first()

	if the_set is not None:
		flagged_subsets = models.Flagged_Subset.query.filter(models.Flagged_Subset.set_id == the_set.id).all()

		##flagged_subsets = db_utils.get_query_results(data_source=None, database='eorlive', table='flagged_subset',
		##												field_tuples=(('set_id', '==', getattr(the_set, 'id')),),
		##												field_sort_tuple=None, output_vars=None)

		low_or_high = the_set.low_or_high
		eor = the_set.eor

		low_high_clause, eor_clause = db_utils.get_lowhigh_and_eor_clauses(low_or_high, eor)

		all_obs_ids_tuples = db_utils.send_query(g.eor_db, '''SELECT starttime
							FROM mwa_setting
							WHERE starttime >= {start} AND starttime <= {end}
							AND projectid='G0009'
							{low_high}
							{eor}
							ORDER BY starttime ASC'''.format(start=the_set.start, end=the_set.end,
															low_high=low_high_clause, eor=eor_clause)).fetchall()

		##all_obs_id_tuples = db_utils.get_query_results(data_source=None, database='eor', table='mwa_setting',
		##									(('starttime', '>=', the_set.start), ('starttime', '<=', the_set.end),
		##									('projectid', '==', 'G0009'),
		##									('obsname', None if the_set.low_or_high == 'any' else 'like', ''.join(the_set.low_or_high, '%')),
		##									('ra_phase_center', None if the_set.eor == 'any' else '==', 0 if the_set.eor == 'EOR0' else 60))
		##									field_sort_tuple=(('starttime', 'asc'),), output_vars=('starttime',))

		all_obs_ids = [tup[0] for tup in all_obs_ids_tuples]

		good_obs_ids_text_file = ''

		for obs_id in all_obs_ids:
			good = True # assume obs_id is good
			for flagged_subset in flagged_subsets:
				if obs_id >= flagged_subset.start and obs_id <= flagged_subset.end: # obs_id is flagged, so it's not good
					good = False
					break
			if good:
				good_obs_ids_text_file = ''.join(good_obs_ids_text_file, str(obs_id), '\n')

		response = make_response(good_obs_ids_text_file)
		filename = ''.join(the_set.name.replace(' ', '_'), '.txt')
		response.headers['Content-Disposition'] = ''.join('attachment; filename=', filename)
		return response
	else:
		return make_response('That set was not found.', 500)

@app.route('/get_filters')
def get_filters():
	users = models.User.query.all()

	##users = db_utils.get_query_results(data_source=None, database='eorlive', table='user',
	##												field_tuples=None field_sort_tuple=None, output_vars=None)

	return render_template('filters.html', users=users)

@app.route('/get_sets', methods = ['POST'])
def get_sets():
	if (g.user is not None and g.user.is_authenticated()):
		request_content = request.get_json()
		set_controls = request_content['set_controls']
		username = set_controls['user']
		eor = set_controls['eor']
		high_low = set_controls['high_low']
		sort = set_controls['sort']
		ranged = set_controls['ranged']

		query = models.Set.query

		if username:
			query = query.filter(models.Set.username == username)

		if eor:
			query = query.filter(models.Set.eor == eor) # eor is 'EOR0' or 'EOR1', which are the values used in the DB

		if high_low:
			query = query.filter(models.Set.low_or_high == high_low) # high_low is 'high' or 'low', which are the
																	 # values used in the DB

		if ranged:
			start_utc = request_content['starttime']
			end_utc = request_content['endtime']
			start_datetime = datetime.strptime(start_utc, '%Y-%m-%dT%H:%M:%SZ')
			end_datetime = datetime.strptime(end_utc, '%Y-%m-%dT%H:%M:%SZ')
			start_gps, end_gps = db_utils.get_gps_from_datetime(start_datetime, end_datetime)
			query = query.filter(and_(models.Set.start >= start_gps,
									models.Set.end <= end_gps))

		if sort:
			if sort == 'hours':
				query = query.order_by(models.Set.total_data_hrs.desc())
			elif sort == 'time':
				query = query.order_by(models.Set.created_on.desc())

		setList = query.all()

		##if ranged:
		##	start_utc = request_content['starttime']
		##	end_utc = request_content['endtime']
		##	start_datetime = datetime.strptime(start_utc, '%Y-%m-%dT%H:%M:%SZ')
		##	end_datetime = datetime.strptime(end_utc, '%Y-%m-%dT%H:%M:%SZ')
		##	start_gps, end_gps = db_utils.get_gps_from_datetime(start_datetime, end_datetime)

		##field_tuples = (('username', '==' if username else None, username), ('eor', '==' if eor else None, eor),
		##					('low_or_high', '==' if high_low else None, high_low),
		##					('start', '>=' if ranged else None, start_gps),
		##					('end', '>=' if ranged else None, end_gps))
	
		##field_sort_tuple = None
		##if sort:
		##	if sort == 'hours'
		##		field_sort_tuple = (('total_data_hrs', 'desc'),)
		##	elif sort == 'time':
		##		field_sort_tuple = (('created_on', 'desc'),)

		##setList = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
		##												field_tuples=field_tuples, field_sort_tuple=field_sort_tuple, output_vars=None)

		include_delete_buttons = request_content['includeDeleteButtons']

		return render_template('setList.html', sets=setList,
			include_delete_buttons=include_delete_buttons)
	else:
		return render_template('setList.html', logged_out=True)

@app.route('/delete_set', methods = ['POST'])
def delete_set():
	if (g.user is not None and g.user.is_authenticated()):
		set_id = request.form['set_id']

		theSet = models.Set.query.filter(models.Set.id == set_id).first()

		##theSet = db_utils.get_query_results(data_source=None, database='eorlive', table='set',
		##												field_tuples=(('id', '==', set_id),),
		##												field_sort_tuple=None, output_vars=None)[0]

		db.session.delete(theSet)
		db.session.commit()
		return 'Success'
	else:
		return redirect(url_for('login'))
