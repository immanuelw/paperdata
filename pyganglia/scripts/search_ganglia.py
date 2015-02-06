#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table

from Tkinter import *
import pyganglia as pyg
import decimal
import getpass

### Script to create GUI to easily search paperdata database
### Has fields to enter to search for any and all which match certain parameters

### Author: Immanuel Washington
### Date: 8-20-14

#create form
def makeform(root, fields):
	entries = []
	for field in fields:
		if field not in []:
			var = IntVar()
			row = Frame(root)
			lab = Label(row, width=15, text=field, anchor='w')
			c = Checkbutton(row, text="Use field as output", variable=var, onvalue = pyg.SEARCH, offvalue = pyg.NOSEARCH)
			c.var = var
			row.pack(side=TOP, fill=X, padx=3, pady=3)

			row2 = Frame(root)
			row2.pack(side=TOP, fill=X, padx=3, pady=3)
			ent = Entry(row2)
			lab2 = Label(row2, width=5, text='Input:', anchor='w')
			lab2.pack(side=LEFT)		

			ivar = IntVar()
			ivar.set(pyg.NONE)
			for key, value in pyg.options().items():
				a = Radiobutton(row2, text=value, variable=ivar, value = key)
				a.var = ivar
				a.pack(side=LEFT)

			lab.pack(side=LEFT)
			c.pack(side=LEFT)
			ent.pack(side=RIGHT, expand=YES, fill=X)
			#creates entries list
			entries.append((field, c.var, a.var, ent))
	return entries

def convert(entries, output, db_dict, var_flo, var_str, var_int, table):
	info_list = []

	#Populate info_list with info that fits pyg module
	for entry in entries:
		field = entry[0]
		search = entry[1].get()
		range_spec = entry[2].get()
		range = entry[3].get()
		#Need to parse range
		if field in [] and range_spec not in [pyg.NONE]:
			if range_spec in [pyg.LIST]:
				ran = range
				range = []
				if len(ran.split(',')) >= 2:
					for item in ran.split(','):
						range.append(item)
			else:
				range = [range]
		
		elif range_spec in [pyg.NONE]:
			range = []

		elif range_spec in [pyg.RANGE]:
			ran = range
			range = []
			if len(ran.split('-')) == 2:
				for item in ran.split('-'):
					try:
						it = int(item)
					except ValueError:
						it = decimal.Decimal(item)
					range.append(it)
			elif len(range.split('-')) == 1:
				try:
					range = [int(range)]
				except ValueError:
					range = [decimal.Decimal(range)]

		elif range_spec in [pyg.LIST]:
			ran = range
			range = []
			if len(ran.split(',')) >= 2:
				for item in ran.split(','):
					try:
						it = int(item)
					except ValueError:
						it = decimal.Decimal(item)
					range.append(it)

		elif range_spec in [pyg.MIN, pyg.MAX, pyg.EXACT]:
			try:
				range = [int(range)]
			except ValueError:
				range = [decimal.Decimal(range)]

		field_info = [field, search, range_spec, range]
		info_list.append(field_info)

	#Decides which output to show user
	limit_query = pyg.fetch(info_list, db_dict, var_flo, var_str, var_int, table) + ' limit 20'
	if output in [sql_string]:
		print pyg.fetch(info_list, db_dict, var_flo, var_str, var_int, table)
	elif output in [dbs_list]:
		print pyg.dbsearch(limit_query)
		print 'Output was limited to 20 entries'
	elif output in [dbs_dict]:
		print pyg.dbsearch_dict(limit_query)
		print 'Output was limited to 20 entries'

	return info_list

if __name__ == '__main__':
	#Decide which table to search
	tab = raw_input('Search which table? -- [monitor_files, iostat, ram, cpu]: ')

	fields = []
	if tab in ['monitor_files']:
		fields.extend(pyg.monitor_files().db_list)
		db_dict = pyg.monitor_files().db_dict
		var_flo = pyg.monitor_files().var_flo
		var_str = pyg.monitor_files().var_str
		var_int = pyg.monitor_files().var_int
		table = pyg.monitor_files().table
	elif tab in ['iostat']:
		fields.extend(pyg.iostat().db_list)
		db_dict = pyg.iostat().db_dict
		var_flo = pyg.iostat().var_flo
		var_str = pyg.iostat().var_str
		var_int = pyg.iostat().var_int
		table = pyg.iostat().table
	elif tab in ['ram']:
		fields.extend(pyg.ram().db_list)
		db_dict = pyg.ram().db_dict
		var_flo = pyg.ram().var_flo
		var_str = pyg.ram().var_str
		var_int = pyg.ram().var_int
		table = pyg.ram().table
	elif tab in ['cpu']:
		fields.extend(pyg.cpu().db_list)
		db_dict = pyg.cpu().db_dict
		var_flo = pyg.cpu().var_flo
		var_str = pyg.cpu().var_str
		var_int = pyg.cpu().var_int
		table = pyg.cpu().table

	decimal.getcontext().prec = 2

	#options for printed output values
	sql_string = 1
	dbs_list = 2
	dbs_dict = 3

	#Generate GUI
	root = Tk()
	root.title('Paperdata Query')
	ents = makeform(root, fields)
	root.bind('<Return>', (lambda event, e=ents: convert(e, sql_string)))
	b1 = Button(root, text='Output String', command=(lambda e=ents: convert(e, sql_string, db_dict, var_flo, var_str, var_int, table)))
	b2 = Button(root, text='Output List', command=(lambda e=ents: convert(e, dbs_list, db_dict, var_flo, var_str, var_int, table)))
	b3 = Button(root, text='Output Dict', command=(lambda e=ents: convert(e, dbs_dict, db_dict, var_flo, var_str, var_int, table)))
	b1.pack(side=LEFT, padx=5, pady=5)
	b2.pack(side=LEFT, padx=5, pady=5)
	b3.pack(side=LEFT, padx=5, pady=5)
	b4 = Button(root, text='Quit', command=root.quit)
	b4.pack(side=LEFT, padx=5, pady=5)
	root.mainloop()

