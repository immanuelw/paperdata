'''
paper.data.scripts.current_db

creates table depicting current summary of paperdata database

author | Immanuel Washington

Functions
---------
current_db | writes table summary of paperdata database into file
'''
import os
import prettytable
from paper.data import dbi as pdbi

def current_db(dbi):
	'''
	writes table summary of paperdata database into file
	summary contraining information about julian days, hosts, etc.

	dbi | object: database interface object
	'''
	with dbi.session_scope() as s:
		FILEs = s.query(pdbi.File).all()
	out_vars = ('era', 'julian_day', 'host', 'base_path', 'filetype', 'init_host')
	current = ((getattr(FILE, out_var) for out_var in out_vars) for FILE in FILEs)

	count = {}
	for entry in current:
		if entry in count.keys():
			count[entry] += 1
		else:
			count[entry] = 1
	out = [key + (value,) for key, value in count.items()]

	x = PrettyTable(['Era', 'Julian Day', 'Host', 'Path', 'Type', 'Initial Host', 'Amount'])
	for line in out:
		x.add_row(line)
	stuff = x.get_string()
	with open(os.path.expanduser('~/paperdata/paper/data/src/table_descr.txt'), 'wb') as df:
		df.write(stuff)

if __name__ == "__main__":
	dbi = pdbi.DataBaseInterface()
	current_db(dbi)
