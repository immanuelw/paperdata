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
	'''
	selects interface and module for input database

	input: database name
	output: database interface object, module object
	'''
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
	'''
	access into database metadata

	input: database name
	output: inspector object
	'''
	dbi, _ = get_dbi(database)
	insp = reflection.Inspector.from_engine(dbi.engine)
	return insp

def get_table_names(database):
	'''
	gets all table names in database

	input: database name
	output: list of table names
	'''
	insp = inspector(database)
	table_names = insp.get_table_names()
	return table_names

def get_column_names(database, table):
	'''
	gets all column names in table

	input: database name, table name
	output: tuple of column names
	'''
	insp = inspector(database)
	#it's a list of dicts
	column_list = insp.get_column_names(table)
	column_names = tuple(column['name'] for column in column_list)
	return column_names

def make_clause(table, field_name, equivalency, value):
	'''
	generates a clause for sqlalchemy filters

	input: table name, field name, type of equivalency, value to compare against
	output: sqlalchemy clause
	'''
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
	'''
	generates a sorting clause for sqlalchemy filter

	input: table, tuple of tuples indicating field to sort and sort order
	output: list of clauses
	'''
	clause_list = [getattr(getattr(table, field_name), field_order)() for field_name, field_order in sort_tuples]
	return clause_list

def group_clause(table, group_tuples):
	'''
	generates a grouping clause for sqlalchemy filter

	input: table, tuple of tuples indicating fields to group
	output: list of clauses
	'''
	clause_list = [getattr(table, field_name) for field_name in field_group_tuples]
	return clause_list

def get_results(s, table, field_tuples, sort_tuples, group_tuples):
	'''
	outputs results from a query after filtering

	input: session object, table name, tuple of field names, equivalency values, and values to limit query, sort tuples and group tuples
	output: list of objects corresponding to filtered query made
	'''
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
	'''
	pulls list of object from database after filtering query

	input: (all optional) -- data source object, database name, table name , field tuples, sort tuples, group tuples
	output: list of objects corresponding to filtered query
	'''
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
