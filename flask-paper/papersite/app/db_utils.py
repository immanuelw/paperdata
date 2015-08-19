from flask import g
from datetime import datetime
from requests_futures.sessions import FuturesSession
from app.flask_app import db
from app import models as edbi
import paperdata_dbi as pdbi
import pyganglia_dbi as pyg
from sqlalchemy import or_, in_
from sqlalchemy.engine import reflection

def get_dbi(database):
	if database == 'paperdata':
		module = pdbi
	elif database == 'ganglia':
		module = pyg
	elif database == 'eorlive':
		module = db
		dbi = edbi
		return dbi, module
	dbi = getattr(module, 'DataBaseInterface')()
	return dbi, module

def inspector(database):
	dbi, _ = get_dbi(database)
	insp = reflection.Inspector.from_engine(dbi.engine)
	return insp

def get_table_names(database):
	insp = inspector(database)
	table_names = insp.get_table_names()
	return table_names

def get_column_names(database, table)
	insp = inspector(database)
	#it's a list of dicts
	column_list = insp.get_column_names(table)
	column_names = tuple(column['name'] for column in column_list)
	return column_names

def make_clause(table, field_name, equivalency, value):
	field = getattr(table, field_name)
	if equivalency is None:
		return None
	if equivalency == '<=':
		clause = field <= value
	elif equivalency == '==':
		clause = field == value
	elif equivalency == '>=':
		clause = field >= value
	elif equivalency == '!=':
		clause = field != value
	elif equivalency == '<':
		clause = field < value
	elif equivalency == '>':
		clause = field > value
	elif equivalency == 'like':
		clause = field.like(value)
	elif equivalency == 'in':
		clause = field.in_(value)
	elif equivalency == 'or' and value is None:
		clause_tuple = tuple(make_clause(table, new_field_name, new_equivalency, new_value)
										for new_field_name, new_equivalency, new_value in field_name)
		clause = or_(*clause_tuple)
	return clause

def sort_clause(table, field_sort_tuple):
	#grab the sort clause of the query
	clause_list = [getattr(getattr(table, field_name), field_order)() for field_name, field_order in field_sort_tuple]
	return clause_list

def get_results(s, table, field_tuples, field_sort_tuple, output_vars):
	results = s.query(table)
	if field_tuples is not None:
		clause_gen = (make_clause(table, field_name, equivalency, value) for field_name, equivalency, value in field_tuples)
		for clause in clause_gen:
			results = results.filter(clause)
	if field_sort_tuple is not None:
		results = results.order_by(*sort_clause(field_sort_tuple)).all()
	if output_vars is not None:
		results = tuple((getattr(entry, output_var) for output_var in output_vars) for entry in results))
	return results

def get_query_results(data_source=None, database=None, table=None, field_tuples=None, field_sort_tuple=None, output_vars=None):
	#field tuples is list of field tuples containting field_name, equivalency, value and in that order
	#ex: [('obs_column', '<=', 23232), ('projectid', '==', 'G0009')]
	if data_source is not None:
		dbi, module = get_dbi(data_source.database)
		table = getattr(module, data_source.table.title())
	elif database is not None:
		dbi, module = get_dbi(database)
		table = getattr(module, table.title())
	else:
		return None

	if table == 'eorlive'.title():
		s = db.session
	else:
		s = dbi.Session()
	results = get_results(s=s, table=table, field_tuples=field_tuples, field_sort_tuple=field_sort_tuple, output_vars=output_vars)
	s.close()
	return results

def get_gps_utc_constants():
	leap_seconds_result = get_query_results(data_source=None, database='eor', table='leap_seconds',
												field_tuples=None, field_sort_tuple=(('leap_seconds', 'desc'),),
												output_vars=('leap_seconds',))[0]

	leap_seconds = leap_seconds_result[0]

	GPS_LEAP_SECONDS_OFFSET = leap_seconds - 19

	jan_1_1970 = datetime(1970, 1, 1)

	jan_6_1980 = datetime(1980, 1, 6)

	GPS_UTC_DELTA = (jan_6_1980 - jan_1_1970).total_seconds()

	return (GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA)

def get_gps_from_datetime(start_datetime, end_datetime):
	session = FuturesSession()

	baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?utciso='

	requestURLStart = ''.join(baseUTCToGPSURL, start_datetime.strftime('%Y-%m-%dT%H:%M:%S'))

	requestURLEnd = ''.join(baseUTCToGPSURL, end_datetime.strftime('%Y-%m-%dT%H:%M:%S'))

	#Start the first Web service request in the background.
	future_start = session.get(requestURLStart)

	#The second request is started immediately.
	future_end = session.get(requestURLEnd)

	#Wait for the first request to complete, if it hasn't already.
	start_gps = int(future_start.result().content)

	#Wait for the second request to complete, if it hasn't already.
	end_gps = int(future_end.result().content)

	return (start_gps, end_gps)

def get_datetime_from_gps(start_gps, end_gps):
	session = FuturesSession()

	baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?gpssec='

	requestURLStart = ''.join(baseUTCToGPSURL, str(start_gps))

	requestURLEnd = ''.join(baseUTCToGPSURL, str(end_gps))

	#Start the first Web service request in the background.
	future_start = session.get(requestURLStart)

	#The second request is started immediately.
	future_end = session.get(requestURLEnd)

	#Wait for the first request to complete, if it hasn't already.
	start_datetime = datetime.strptime(future_start.result().content.decode('utf-8'), '"%Y-%m-%dT%H:%M:%S"')

	#Wait for the second request to complete, if it hasn't already.
	end_datetime = datetime.strptime(future_end.result().content.decode('utf-8'), '"%Y-%m-%dT%H:%M:%S"')

	return (start_datetime, end_datetime)
