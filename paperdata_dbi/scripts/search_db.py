#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table

from Tkinter import *
import paperdata_db as pdb
import paperdata_dbi as pdbi
import decimal
import sys
import time
import csv

### Script to create GUI to easily search paperdata database
### Has fields to enter to search for any and all which match certain parameters

### Author: Immanuel Washington
### Date: 8-20-14

#create form
def makeform(root, fields, file_fields, obs_fields):
	entries = []
	for field in fields:
		row2 = Frame(root)
		row2.pack(side=TOP, fill=X, padx=3, pady=3)
		ent = Entry(row2)
		lab2 = Label(row2, width=5, text='', anchor='w')
		lab2.pack(side=LEFT)		

		ivar = IntVar()
		ivar.set(pdb.NONE)
		for key, value in pdb.options.items():
			a = Radiobutton(row2, text=value, variable=ivar, value=key)
			a.var = ivar
			a.pack(side=LEFT)

		lab.pack(side=LEFT)
		c.pack(side=LEFT)
		ent.pack(side=RIGHT, expand=YES, fill=X)
		#creates entries list
		entries.append((field, a.var, ent))

	output = []

	row3 = Frame(root)
	row3.pack(side=TOP, fill=X, padx=3, pady=3)
	ent2 = Entry(row3)
	lab3 = Label(row3, width=5, text='', anchor='w')
	lab3.pack(side=LEFT)		
	for field in file_fields:
		jvar = IntVar()
		jvar.set(False)
		d = Checkbutton(row3, text=field, variable=jvar, onvalue=True, offvalue=False)
		d.var = jvar
		d.pack(side=LEFT)
		file_output.append((field, d.var, ent2))
	ent2.pack(side=RIGHT, expand=YES, fill=X)

	row4 = Frame(root)
	row4.pack(side=TOP, fill=X, padx=3, pady=3)
	ent3 = Entry(row4)
	lab4 = Label(row4, width=5, text='', anchor='w')
	lab4.pack(side=LEFT)		
	for field in obs_fields:
		kvar = IntVar()
		kvar.set(False)
		e = Checkbutton(row4, text=field, variable=kvar, onvalue=True, offvalue=False)
		e.var = kvar
		e.pack(side=LEFT)
		obs_output.append((field, e.var, ent3))
	ent3.pack(side=RIGHT, expand=YES, fill=X)

	return (entries, file_output, obs_output)

def convert(entries):
	info_list = []

	#Populate info_list with info that fits pdb module
	for entry in entries:
		field = entry[0]
		field_range_spec = entry[1].get()
		field_range = entry[2].get()
		#Need to parse field_range
		if field in [] and field_range_spec not in [pdb.NONE]:
			if field_range_spec in [pdb.LIST]:
				ran = field_range
				field_range = []
				if len(ran.split(',')) >= 2:
					for item in ran.split(','):
						field_range.append(item)
			else:
				field_range = [field_range]
		
		elif field_range_spec in [pdb.NONE]:
			field_range = []

		elif field_range_spec in [pdb.RANGE]:
			ran = field_range
			field_range = []
			if len(ran.split('-')) == 2:
				for item in ran.split('-'):
					try:
						it = int(item)
					except ValueError:
						it = decimal.Decimal(item)
					field_range.append(it)
			elif len(field_range.split('-')) == 1:
				try:
					field_range = [int(field_range)]
				except ValueError:
					field_range = [decimal.Decimal(field_range)]

		elif field_range_spec in [pdb.LIST]:
			ran = field_range
			field_range = []
			if len(ran.split(',')) >= 2:
				for item in ran.split(','):
					try:
						it = int(item)
					except ValueError:
						it = decimal.Decimal(item)
					field_range.append(it)

		elif field_range_spec in [pdb.MIN, pdb.MAX, pdb.EXACT]:
			try:
				field_range = [int(field_range)]
			except ValueError:
				field_range = [decimal.Decimal(field_range)]

		field_info = (field, field_range_spec, field_range)
		info_list.append(field_info)

	return info_list

def match(input_group, tab, field, value, equality):
	if equality is '=':
		if tab in ('File',):
			FILEs = input_group
			if field is 'host':
				return tuple(FILE for FILE in FILEs if FILE.host==value)
			elif field is 'path':
				return tuple(FILE for FILE in FILEs if FILE.path==value)
			elif field is 'filename':
				return tuple(FILE for FILE in FILEs if FILE.filename==value)
			elif field is 'filetype':
				return tuple(FILE for FILE in FILEs if FILE.filetype==value)
			elif field is 'obsnum':
				return tuple(FILE for FILE in FILEs if FILE.obsnum==value)
			elif field is 'filesize':
				return tuple(FILE for FILE in FILEs if FILE.filesize==value)
			elif field is 'write_to_tape':
				return tuple(FILE for FILE in FILEs if FILE.write_to_tape==value)
			elif field is 'delete_file':
				return tuple(FILE for FILE in FILEs if FILE.delete_file==value)
		elif tab in ('Observation',):
			OBSs = input_group
			if field is 'obsnum':
				return tuple(OBS for OBS in OBSs if OBS.obsnum==value)
			elif field is 'julian_date':
				return tuple(OBS for OBS in OBSs if OBS.julian_date==value)
			elif field is 'polarization':
				return tuple(OBS for OBS in OBSs if OBS.polarization==value)
			elif field is 'julian_day':
				return tuple(OBS for OBS in OBSs if OBS.julian_day==value)
			elif field is 'era':
				return tuple(OBS for OBS in OBSs if OBS.era==value)
			elif field is 'era_type':
				return tuple(OBS for OBS in OBSs if OBS.era_type==value)
	elif equality is '<':
		if tab in ('File',):
			FILEs = input_group
			if field is 'obsnum':
				return tuple(FILE for FILE in FILEs if FILE.obsnum<=value)
			elif field is 'filesize':
				return tuple(FILE for FILE in FILEs if FILE.filesize<=value)
		elif tab in ('Observation',):
			OBSs = input_group
			if field is 'obsnum':
				return tuple(OBS for OBS in OBSs if OBS.obsnum<=value)
			elif field is 'julian_date':
				return tuple(OBS for OBS in OBSs if OBS.julian_date<=value)
			elif field is 'julian_day':
				return tuple(OBS for OBS in OBSs if OBS.julian_day<=value)
			elif field is 'era':
				return tuple(OBS for OBS in OBSs if OBS.era<=value)
	elif equality is '>':
		if tab in ('File',):
			FILEs = input_group
			if field is 'obsnum':
				return tuple(FILE for FILE in FILEs if FILE.obsnum>=value)
			elif field is 'filesize':
				return tuple(FILE for FILE in FILEs if FILE.filesize>=value)
		elif tab in ('Observation',):
			OBSs = input_group
			if field is 'obsnum':
				return tuple(OBS for OBS in OBSs if OBS.obsnum>=value)
			elif field is 'julian_date':
				return tuple(OBS for OBS in OBSs if OBS.julian_date>=value)
			elif field is 'julian_day':
				return tuple(OBS for OBS in OBSs if OBS.julian_day>=value)
			elif field is 'era':
				return tuple(OBS for OBS in OBSs if OBS.era>=value)

def sql_query(output, info_list, tab):
	eq_dict = {pdb.EXACT:'=', pdb.MAX:'<', pdb.MIN:'>'}
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	s.close()
	if tab in ('File',):
		QUERYs = s.query(pdbi.File).all()
	elif tab in ('Observation',):
		QUERYs = s.query(pdbi.Observation).all()
	for item in info_list:
		field = item[0]
		range_spec = item[1]
		value = item[2]
		if range_spec in eq_dict.keys():
			QUERYs = match(QUERYs, tab, field, value, eq_dict[range_spec])
		elif range_spec is pdb.LIST:
			for element in value:
				QUERYs = match(QUERYs, tab, field, element, '=')
		elif range_spec in pdb.RANGE:
			QUERYs = match(QUERYs, tab, field, value[0], '>')
			QUERYs = match(QUERYs, tab, field, value[1], '<')
		else:
			continue

	sql_output = []
	for QUERY in QUERYs:
		query_dict = QUERY.__dict__
		out = tuple(query_dict[field_out] for field_out in output)
		sql_output.append(out)

	sql_output = tuple(sql_output)
	return sql_output

def write_to_file(data, table):
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	file_name = './paper_{0}_output_{1}.psv'.format(table, time_date)
	data_file = open(file_name,'wb')
	wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')
	for item in data:
		wr.writerow(item)
	data_file.close()

	print file_name + ' saved'
	return None

def print_data(data):
	for item in data:
		print ', '.join(map(str, item))

	return None

if __name__ == '__main__':
	#Decide which table to search
	if len(sys.argv) > 1:
		tab = sys.argv[1]
	else:
		tab = raw_input('Search which table? -- [File, Observation]: ')
	
	if tab not in pdb.classes:
		sys.exit()

	if tab in ('File',):
		non_fields = ('full_path', 'md5sum', 'tape_index')
		fields = tuple(field for field in pdb.instant_class[tab].db_list if field not in non_fields)
	elif tab in ('Observation',):
		non_fields = ('length', 'time_start', 'time_end', 'delta_time', 'prev_obs', 'next_obs')
		fields = tuple(field for field in pdb.instant_class[tab].db_list if field not in non_fields)

	file_fields = pdb.instant_class['File'].db_list
	obs_fields = pdb.instant_class['Observation'].db_list

	decimal.getcontext().prec = 2

	#Generate GUI
	root = Tk()
	root.title('Paperdata Query')
	ents, file_output_conv, obs_output_conv = makeform(root, fields, file_fields, obs_fields)
	file_output = tuple(field[0] for field in file_output_conv if field[1].get() is True)
	obs_output = tuple(field[0] for field in obs_output_conv if field[1].get() is True)
	info_list = convert(ents)
	b1 = Button(root, text='Output Files', command=(lambda e=ents: print_data(sql_query(file_output, convert(e), tab))))
	b2 = Button(root, text='Output Observations', command=(lambda e=ents: print_data(sql_query(obs_output, convert(e), tab))))
	b3 = Button(root, text='Output Files as .psv', command=(lambda e=ents: write_to_file(sql_query(file_output, convert(e), tab), tab)))
	b4 = Button(root, text='Output Observations as .psv', command=(lambda e=ents: write_to_file(sql_query(obs_output, convert(e), tab), tab)))
	b5 = Button(root, text='Quit', command=root.quit)
	b1.pack(side=LEFT, padx=5, pady=5)
	b2.pack(side=LEFT, padx=5, pady=5)
	b3.pack(side=LEFT, padx=5, pady=5)
	b4.pack(side=LEFT, padx=5, pady=5)
	b5.pack(side=LEFT, padx=5, pady=5)
	root.mainloop()

