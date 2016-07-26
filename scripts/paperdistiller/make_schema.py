'''
scripts.paperdistiller.make_schema

generates schema file of paperdistiller database

author | Immanuel Washington
'''
from __future__ import print_function
from paper import schema

if __name__ == '__main__':
    schema.make_schema(db='paperdistiller')
