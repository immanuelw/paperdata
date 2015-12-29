'''
paper.data.scripts.schema_db

generates schema file of paperdata database

author | Immanuel Washington
'''
import os
import paper.schema as schema
from paper.data import data_db as pdb

if __name__ == '__main__':
    filename = os.path.expanduser('~/paperdata/paper/data/src/schema.txt')
    schema.schema_db(pdb, filename)
