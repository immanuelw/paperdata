#!/usr/bin/env python
import os
import prettytable
import dbi as pdbi

#script to show state of paperdata

def main():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(pdbi.File).all()
	s.close()
	out_vars = ('era', 'julian_day', 'host', 'path', 'filetype', 'source_host')
	current = tuple((getattr(FILE, out_var) for out_var in out_vars) for FILE in FILEs)

	count = {}
	for entry in current:
		if entry in count.keys():
			count[entry] += 1
		else:
			count[entry] = 1
	out = []
	for key, value in count.items():
		out.append(key + (value,))
		

	x = PrettyTable(['Era', 'Julian Day', 'Host', 'Path', 'Type', 'Source Host', 'Amount'])
	for line in out:
		x.add_row(line)
	stuff = x.get_string()
	with open(os.path.expanduser('~/src/table_descr.txt'), 'wb') as df:
		df.write(stuff)

if __name__ == "__main__":
	main()

