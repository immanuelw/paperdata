'''
paper.ganglia.scripts.schema_db

generates schema file of ganglia database

author | Immanuel Washington
'''
import os
import paper.schema as schema
from paper.ganglia import ganglia_db as gdb

if __name__ == '__main__':
    filename = os.path.expanduser('~/paperdata/paper/ganglia/src/schema.txt')
    schema.schema_db(gdb, filename)
