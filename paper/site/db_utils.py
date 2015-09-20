from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg
from paper.site.search import models as sdbi
from paper.site.admin import models as adbi
from paper.site.search.flask_app import db
from paper.site.admin.flask_app import db
from sqlalchemy import or_
from sqlalchemy.engine import reflection
import socket

def get_dbi(database):
	host = socket.gethostname()
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
	elif database == 'search':
		module = sdbi
		dbi = None
		return dbi, module
	elif database == 'admin':
		module = adbi
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

def get_results(s, table, field_tuples, sort_tuples, group_tuples):
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

	return results

def query(data_source=None, database=None, table=None, field_tuples=None, sort_tuples=None, group_tuples=None):
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

	if database == 'search':
		s = sdb.session
	elif database == 'admin':
		s = adb.session
	else:
		s = dbi.Session()
	results = get_results(s=s, table=table, field_tuples=field_tuples, sort_tuples=sort_tuples, group_tuples=group_tuples)
	s.close()
	return results
