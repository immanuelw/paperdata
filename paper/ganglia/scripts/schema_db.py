from __future__ import print_function
import sys
import os
import prettytable
from paper.ganglia import ganglia_db as gdb

def main():
	filename = os.path.expanduser('~/paperdata/paper/ganglia/src/schema.txt')
	with open(filename, 'wb') as df:
		print('Starting ..')
	var_classes = gdb.all_classes

	for var_class in var_classes:
		x = prettytable.PrettyTable(['Field', 'Type', 'Default', 'Unique Key', 'Description'])
		with open(filename, 'ab') as df:
			df.write(var_class.table + '\n')
		for field in var_class.db_list:
			full_item = [field, var_class.db_descr[field]['type'], var_class.db_descr[field]['default'],
								var_class.db_descr[field]['key'], var_class.db_descr[field]['description']]
			x.add_row(full_item)
			stuff = x.get_string()
		with open(filename, 'ab') as df:
			df.write(stuff)
			df.write('\n')

	print('Done!')

if __name__ == "__main__":
	main()

