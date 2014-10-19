#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table

### Script to create GUI to easily search paperdata database
### Has fields to enter to search for any and all which match certain parameters

### Author: Immanuel Washington
### Date: 8-20-14

from Tkinter import *
#from paperdataDB import *
import paperdataDB as pdb
import decimal

fields = pdb.fields()

#create form
def makeform(root, fields):
	entries = []
	for field in fields:
		if field not in ['md5sum', 'restore_history', 'data_length', 'cal_location', 'tape_location', 'ready_to_tape','delete_file']:
			var = IntVar()
			row = Frame(root)
			lab = Label(row, width=15, text=field, anchor='w')
			c = Checkbutton(row, text="Use field as output", variable=var, onvalue = pdb.SEARCH, offvalue = pdb.NOSEARCH)
			c.var = var
			row.pack(side=TOP, fill=X, padx=3, pady=3)

			row2 = Frame(root)
			row2.pack(side=TOP, fill=X, padx=3, pady=3)
			ent = Entry(row2)
			lab2 = Label(row2, width=5, text='Input:', anchor='w')
			lab2.pack(side=LEFT)		

			ivar = IntVar()
			ivar.set(pdb.NONE)
			for key, value in pdb.options().items():
				a = Radiobutton(row2, text=value, variable=ivar, value = key)
				a.var = ivar
				a.pack(side=LEFT)

			lab.pack(side=LEFT)
			c.pack(side=LEFT)
			ent.pack(side=RIGHT, expand=YES, fill=X)
			#creates entries list
			entries.append((field, c.var, a.var, ent))
	return entries

decimal.getcontext().prec = 5

def convert(entries):
	info_list = []

	#Populate info_list with info that fits pdb module
	for entry in entries:
		field = entry[0]
		search = entry[1].get()
		range_spec = entry[2].get()
		range = entry[3].get()
		#Need to parse range
		if range_spec == pdb.NONE:
			range = []

		elif range_spec == pdb.RANGE:
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

		elif range_spec == pdb.LIST:
			ran = range
			range = []
			if len(ran.split(',')) >= 2:
				for item in ran.split(','):
					try:
						it = int(item)
					except ValueError:
						it = decimal.Decimal(item)
					range.append(it)

		elif range_spec in [pdb.MIN, pdb.MAX, pdb.EXACT]:
			try:
				range = [int(range)]
			except ValueError:
				range = [decimal.Decimal(range)]

		field_info = [field, search, range_spec, range]
		info_list.append(field_info)

	print pdb.fetch(info_list)
	return info_list

if __name__ == '__main__':
	root = Tk()
	root.title('Paperdata Query')
	ents = makeform(root, fields)
	root.bind('<Return>', (lambda event, e=ents: convert(e)))
	b1 = Button(root, text='Show', command=(lambda e=ents: convert(e)))
	b1.pack(side=LEFT, padx=5, pady=5)
	b2 = Button(root, text='Quit', command=root.quit)
	b2.pack(side=LEFT, padx=5, pady=5)
	root.mainloop()

