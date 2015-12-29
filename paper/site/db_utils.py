'''
paper.site.db_utils

author | Immanuel Washington

Functions
---------
get_dbi | gets database interface and module to use
inspector | allows inspection of database through object
get_table_names | gets table names in database
get_column_names | gets names of fields in table
make_clause | creates full sqlalchemy clause
sort_clause | creates sort clause for query
group_clause | creates group clause for query
get_results | processes all clauses in query to output object list
make_sess | creates database session
query | queries database
'''
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg
from paper.site.search import models as sdbi
from paper.site.admin import models as adbi
from paper.site.flask_app import search_db as sdb
from paper.site.flask_app import admin_db as adb
from sqlalchemy import or_
from sqlalchemy.engine import reflection
import socket

def get_dbi(database):
    '''
    selects interface and module for input database

    Parameters
    ----------
    database | str: database name

    Returns
    -------
    tuple:
        object: database interface object
        object: module object
    '''
    host = socket.gethostname()
    if database == 'paperdata':
        module = pdbi
        configfile = '~/paperdata/paperdata.cfg'
    elif database == 'ganglia':
        module = pyg
        configfile = '~/paperdata/ganglia.cfg'
    elif database == 'search':
        module = sdbi
        dbi = None
        return dbi, module
    elif database == 'admin':
        module = adbi
        dbi = None
        return dbi, module
    dbi = module.DataBaseInterface(configfile=configfile)

    return dbi, module

def inspector(database):
    '''
    access into database metadata

    Parameters
    ----------
    database | str: database name

    Returns
    -------
    object: inspector object
    '''
    dbi, _ = get_dbi(database)
    insp = reflection.Inspector.from_engine(dbi.engine)

    return insp

def get_table_names(database):
    '''
    gets all table names in database

    Parameters
    ----------
    database | str: database name

    Returns
    -------
    list[str]: table names
    '''
    insp = inspector(database)
    table_names = insp.get_table_names()

    return table_names

def get_column_names(database, table):
    '''
    gets all column names in table

    Parameters
    ----------
    database | str: database name
    table | str: table name

    Returns
    -------
    list[str]: column names
    '''
    insp = inspector(database)
    #it's a list of dicts
    column_list = insp.get_column_names(table)
    column_names = tuple(column['name'] for column in column_list)

    return column_names

def make_clause(table, field_name, equivalency, value):
    '''
    generates a clause for sqlalchemy filters

    Parameters
    ----------
    table | str: table name
    field_name | str: field name
    equivalency | str: type of equivalency
    value | int/float/str: value to compare against

    Returns
    -------
    object?: sqlalchemy clause
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

    Parameters
    ----------
    table | str: table name
    sort_tuples | list[tuple[str]]: tuples indicating field to sort and sort order

    Returns
    -------
    list[object]: clauses
    '''
    clause_list = [getattr(getattr(table, field_name), field_order)() for field_name, field_order in sort_tuples]

    return clause_list

def group_clause(table, group_tuples):
    '''
    generates a grouping clause for sqlalchemy filter

    Parameters
    ----------
    table | str: table name
    group_tuples | list[tuple[str]]: tuples indicating fields to group

    Returns
    -------
    list[object]: clauses
    '''
    clause_list = [getattr(table, field_name) for field_name in field_group_tuples]

    return clause_list

def get_results(s, table, field_tuples, sort_tuples, group_tuples):
    '''
    outputs results from a query after filtering

    Parameters
    ----------
    s | object: session object
    table | str: table name
    field_tuples | list[tuple[str]]: tuples of field names, equivalency values, and values to limit query
    sort_tuples | list[tuple[str]]: tuples indicating field to sort and sort order
    group_tuples | list[tuple[str]]: tuples indicating fields to group

    Returns
    -------
    list[object]: objects corresponding to filtered query made
    '''
    results = s.query(table)
    if field_tuples is not None:
        clause_gen = (make_clause(table, field_name, equivalency, value) for field_name, equivalency, value in field_tuples)
        for clause in clause_gen:
            if clause is not None:
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

    return results.all()

def make_sess(database=None, dbi=None):
    '''
    gets database interface session object

    Parameters
    ----------
    database | Optional[str]: database name --defaults to None
    dbi | Optional[object]: database interface object --defaults to None

    Returns
    -------
    object: session object
    '''

    if database == 'search':
        s = sdb.session
    elif database == 'admin':
        s = adb.session
    else:
        s = dbi.Session()

    return s

def query(data_source=None, database=None, table=None, field_tuples=None, sort_tuples=None, group_tuples=None):
    '''
    pulls list of object from database after filtering query

    Parameters
    ----------
    data_source | Optional[object]: data source object --defaults to None
    database | Optional[str]: database name --defaults to None
    table | Optional[str]: table name --defaults to None
    field_tuples | Optional[list]: tuples of field names, equivalency values, and values to limit query -- defaults to None
    sort_tuples | Optional[list]: tuples indicating field to sort and sort order --defaults to None
    group_tuples | Optional[list]: tuples indicating fields to group --defaults to None

    Returns
    -------
    list[object]: objects corresponding to filtered query
    '''
    if data_source is not None:
        dbi, module = get_dbi(data_source.database)
        table = getattr(module, data_source.table)
    elif database is not None:
        dbi, module = get_dbi(database)
        table = getattr(module, table)
    else:
        dbi, module = get_dbi(getattr(data_source, 'database'))
        table = getattr(data_source, 'table')

    s = make_sess(database, dbi)
    results = get_results(s=s, table=table, field_tuples=field_tuples, sort_tuples=sort_tuples, group_tuples=group_tuples)
    s.close()

    return results
