'''
paper.schema

generates schema file of database

author | Immanuel Washington
'''
from __future__ import print_function
import os
import prettytable
from sqlalchemy import Enum, ColumnDefault
from paper.data import dbi as pdbi
from paper.distiller import dbi as ddbi
from paper.ganglia import dbi as pyg

def make_schema(db):
    '''
    Makes database schema file

    db | str: name of database
    '''
    if db == 'paperdata':
        xdbi = pdbi
    elif db == 'paperdistiller':
        xdbi = ddbi
    elif db == 'ganglia':
        xdbi = pyg

    meta = xdbi.Base.metadata

    filename = '{db}_schema.txt'.format(db=db)
    file_dir = os.path.dirname(__file__)
    schema_file = os.path.realpath(os.path.join(file_dir, '../doc', filename))

    with open(schema_file, 'w') as sf:
        print('Starting ..')
        for table in meta.sorted_tables:
            schema_table = prettytable.PrettyTable(['Field', 'Type', 'Default', 'Key', 'Description'])
            sf.write(''.join((table.name, '\n')))
            foreign_keys = [fk.column.name for fk in table.foreign_keys]
            for column in table.columns:
                key = ''
                if column.primary_key:
                    key = 'PRIMARY'
                elif column.name in foreign_keys:
                    key = 'FOREIGN'
                elif column.unique:
                    key = 'UNIQUE'

                default = ''
                if type(column.default) == ColumnDefault:
                    default = 'FUNCTION DEFAULT'
                elif column.default:
                    default = column.default

                doc = ''
                if column.doc:
                    doc = column.doc

                col_type = column.type
                if isinstance(column.type, Enum):
                    col_type = 'Enum'

                full_item = [column.name, col_type, default, key, doc]
                schema_table.add_row(full_item)
                stuff = schema_table.get_string()
            sf.write(stuff)
            sf.write('\n')
        print('Done!')

if __name__ == '__main__':
    print('This is just a module')
