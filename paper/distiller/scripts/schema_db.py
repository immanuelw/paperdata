import os
import paper.schema as schema
from paper.distiller import distill_db as ddb

if __name__ == "__main__":
	filename = os.path.expanduser('~/paperdata/paper/distiller/src/schema.txt')
	schema.schema_db(ddb, filename)
