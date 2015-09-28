#!/usr/bin/env python
import os
import prettytable
from paper.data import dbi as pdbi

#script to show state of paper

def main():
	dbi = pdbi.DataBaseInterface()
	with dbi.session_scope() as s:
		FILEs = s.query(pdbi.File).all()
	out_vars = ('era', 'julian_day', 'host', 'path', 'filetype', 'source_host')
	current = tuple((getattr(FILE, out_var) for out_var in out_vars) for FILE in FILEs)

	count = {}
	for entry in current:
		if entry in count.keys():
			count[entry] += 1
		else:
			count[entry] = 1
	out = [key + (value,) for key, value in count.items()]

	x = PrettyTable(['Era', 'Julian Day', 'Host', 'Path', 'Type', 'Source Host', 'Amount'])
	for line in out:
		x.add_row(line)
	stuff = x.get_string()
	with open(os.path.expanduser('~/paper/data/src/table_descr.txt'), 'wb') as df:
		df.write(stuff)

if __name__ == "__main__":
	main()

