'''
scripts.make_schemas

generates schema files of databases

author | Immanuel Washington
'''
from __future__ import print_function
from paper import schema

if __name__ == '__main__':
    schema.make_schema(db='paperdata')
    schema.make_schema(db='paperdistiller')
    schema.make_schema(db='ganglia')
