import sys
import prettytable
import paperdata_db as pdb

def main():
	with open('../src/schema.txt', 'wb') as df:
		print 'Starting ..'
	var_classes = pdb.all_classes

	for var_class in var_classes:
		x = prettytable.PrettyTable(['Field', 'Type', 'Default', 'Unique Key', 'Description'])
		with open('../src/schema.txt', 'ab') as df:
			df.write(var_class.table + '\n')
		for field in var_class.db_list:
			full_item = [field, var_class.db_descr[field][0], var_class.db_descr[field][1], var_class.db_descr[field][2], var_class.db_descr[field][3]]
			x.add_row(full_item)
			stuff = x.get_string()
		with open('../src/schema.txt', 'ab') as df:
			df.write(stuff)
			df.write('\n')

	print 'Done!'

if __name__ == "__main__":
	main()

