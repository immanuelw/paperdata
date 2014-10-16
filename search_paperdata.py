#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table

### Script to create GUI to easily search paperdata database
### Has fields to enter to search for any and all which match certain parameters

### Author: Immanuel Washington
### Date: 8-20-14

from Tkinter import *
#from paperdataDB.paperdataDB import *
import paperdataDB.paperdataDB as pdb

fields = pdb.fields()

#create form
def makeform(root, fields):
	entries = []
	for field in fields:
		if field not in ['md5sum', 'restore_history', 'data_length', 'cal_location', 'tape_location', 'ready_to_tape','delete_file']:
			var = IntVar()
			row = Frame(root)
			lab = Label(row, width=15, text=field, anchor='w')
			ent = Entry(row)
			c = Checkbutton(row, text="Search field", variable=var, onvalue = pdb.SEARCH, offvalue = pdb.NOSEARCH)
			c.var = var
			row.pack(side=TOP, fill=X, padx=3, pady=3)
			row2 = Frame(root)
			row2.pack(side=TOP, fill=X, padx=3, pady=3)

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

def convert(entries):
	for entry in entries:
		print entry[1].get()
		print entry[2].get()
		print entry[3].get()

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

