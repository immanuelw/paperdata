from flask import g
from datetime import datetime
from requests_futures.sessions import FuturesSession
from app.flask_app import db
from app import models as edbi
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg
from paper.convert import gcal2jd
from sqlalchemy import or_
from sqlalchemy.engine import reflection
import socket

host = socket.gethostname()

def get_dbi(database):
	if database == 'paper':
		module = pdbi
		configfile = '/mnt/paperdata/paperdata.cfg'
		if host == 'seharu':
			configfile = '~/paperdata/paperdata.cfg'
	elif database == 'ganglia':
		module = pyg
		configfile = '/mnt/paperdata/ganglia.cfg'
		if host == 'seharu':
			configfile = '~/paperdata/ganglia.cfg'
	elif database == 'eorlive':
		module = edbi
		dbi = None
		return dbi, module
	dbi = getattr(module, 'DataBaseInterface')(configfile=configfile)
	return dbi, module

def inspector(database):
	dbi, _ = get_dbi(database)
	insp = reflection.Inspector.from_engine(dbi.engine)
	return insp

def get_table_names(database):
	insp = inspector(database)
	table_names = insp.get_table_names()
	return table_names

def get_column_names(database, table):
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

def sort_clause(table, sort_tuples):
	#grab the sort clause of the query
	clause_list = [getattr(getattr(table, field_name), field_order)() for field_name, field_order in sort_tuples]
	return clause_list

def group_clause(table, group_tuples):
	#grab the group clause of the query
	clause_list = [getattr(table, field_name) for field_name in field_group_tuples]
	return clause_list

def get_results(s, table, field_tuples, sort_tuples, group_tuples, output_vars):
	results = s.query(table)
	if field_tuples is not None:
		clause_gen = (make_clause(table, field_name, equivalency, value) for field_name, equivalency, value in field_tuples)
		for clause in clause_gen:
			results = results.filter(clause)
	if group_tuples is not None:
		if sort_tuples is not None:
			order_first = results.order_by(*sort_clause(table, sort_tuples)).subquery()
			results = s.query().add_entity(table, alias=order_first).group_by(*group_clause(table, group_tuples))
		else:
			results = results.group_by(*group_clause(table, group_tuples))
	else:
		if sort_tuples is not None:
			results = results.order_by(*sort_clause(table, sort_tuples))

	results = results.all()

	#if output_vars is not None:
		#results = tuple((getattr(entry, output_var) for output_var in output_vars) for entry in results)
	return results

def query(data_source=None, database=None, table=None, field_tuples=None, sort_tuples=None, group_tuples=None, output_vars=None):
	#field tuples is list of field tuples containting field_name, equivalency, value and in that order
	#ex: [('obs_column', '<=', 23232), ('projectid', '==', 'G0009')]
	if data_source is not None:
		dbi, module = get_dbi(data_source.database)
		table = getattr(module, data_source.table.title())
	elif database is not None:
		dbi, module = get_dbi(database)
		table = getattr(module, table.title())
	else:
		dbi, module = get_dbi(getattr(data_source, 'database'))
		table = getattr(data_source, 'table')

	if database == 'eorlive' or not data_source is None:
		s = db.session
	else:
		s = dbi.Session()
	results = get_results(s=s, table=table, field_tuples=field_tuples, sort_tuples=sort_tuples, group_tuples=group_tuples,
							output_vars=output_vars)
	s.close()
	return results

def get_set_strings():
	pol_strs = ('all', 'xx', 'xy', 'yx', 'yy')
	era_type_strs = ('all',)
	host_strs = ('all', 'pot1', 'pot2', 'pot3', 'folio', 'pot8', 'nas1')
	filetype_strs = ('all', 'uv', 'uvcRRE', 'npz')
	return (pol_strs, era_type_strs, host_strs, filetype_strs)

def get_jd_from_datetime(start_time=None, end_time=None):
	time_start = None
	time_end = None
	if isinstance(start_time, str):
		start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
	if isinstance(end_time, str):
		end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')
	if start_time is not None:
		time_start = gcal2jd(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute, start_time.second)
	if end_time is not None:
		time_end = gcal2jd(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute, end_time.second)
	if time_end is None:
		return time_start
	return time_start, time_end

def get_utc_from_datetime(date_time):
	utc = (date_time - datetime(1970, 1, 1)).total_seconds()
	return utc
