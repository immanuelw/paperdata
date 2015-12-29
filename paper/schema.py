'''
paper.schema

author | Immanuel Washington

Functions
---------
schema_db | creates a text file containing a table-like represetation of a database
'''
from __future__ import print_function
import sys
import os
import prettytable

def schema_db(xdb, schema_file):
    '''
    creates database schema file

    Parameters
    ----------
    xdb | object: module containing the schema data
    schema_file | str: path of output file for schema
    '''
    with open(schema_file, 'wb') as sf:
        print('Starting ..')
        for var_class in xdb.all_classes:
            schema_table = prettytable.PrettyTable(['Field', 'Type', 'Default', 'Key', 'Description'])
            sf.write(''.join((var_class.table, '\n')))
            for field in var_class.db_list:
                full_item = [field, var_class.db_descr[field]['type'], var_class.db_descr[field]['default'],
                                    var_class.db_descr[field]['key'], var_class.db_descr[field]['description']]
                schema_table.add_row(full_item)
                stuff = schema_table.get_string()
            sf.write(stuff)
            sf.write('\n')

    print('Done!')

if __name__ == "__main__":
    print('This is a module only!')
