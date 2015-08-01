#!/usr/bin/env python
import prettytable

#script to show state of paperdata

def main():
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT era, julian_day, SUBSTRING_INDEX(raw_path, 'z', 1), SUBSTRING_INDEX(path, 'z', 1), count(*) from paperdata group by julian_day, SUBSTRING_INDEX(raw_path, 'z', 1), SUBSTRING_INDEX(path, 'z', 1) order by julian_day ASC''')
	results = cursor.fetchall()

	cursor.close()
	connection.close()

	x = PrettyTable(["Era", "Julian Day", "Raw Path", "Compressed Path", "Type(R/C/B)", "Amount"])
	for item in results:
	if item[2] != 'NULL' and item[3] == 'NULL':
		file_type = 'R'
	elif item[3] != 'NULL' and item[2] == 'NULL':
		file_type = 'C'
	elif item[2] != 'NULL' and item[3] != 'NULL':
		file_type = 'B'
	full_item = item[:4] + (file_type, item[4])
	x.add_row(full_item)
	stuff = x.get_string()
	with open('../src/table_descr.txt', 'wb') as df:
		df.write(stuff)

if __name__ == "__main__":
	main()

