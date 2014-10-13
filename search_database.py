#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
from Tkinter import *
import os

### Script to create GUI to easily search paperdata database
### Has fields to enter to search for any and all which match certain parameters

### Author: Immanuel Washington
### Date: 8-20-14

#initalize lists to create gui with

fields = 'host', 'path', 'era', 'era type', 'obsnum', 'Julian Day', 'Julian Date', 'polarization', 'Length of data', 'raw location', 'Calibrate file', 'tape location', 'file size', 'Compressed', 'Ready to Tape', 'Ready for deletion'

mins = 'N/A', 'N/A', 'min', 'N/A', 'min', 'min', 'min', 'N/A', 'min', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'

maxs = 'N/A', 'N/A', 'max', 'N/A', 'max', 'max', 'max', 'N/A', 'max', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'

vtext = {fields[0]:'host', fields[1]:'path', fields[2]:'era', fields[3]:'era_type', fields[4]:'obsnum', fields[5]:'julian_day', fields[6]:'julian_date', fields[7]:'polarization', fields[8]:'data_length', fields[9]:'raw_location', fields[10]:'cal_location', fields[11]:'tape_location', fields[12]:'file_size', fields[13]:'compressed', fields[14]:'ready_to_tape', fields[15]:'delete_file'}

#allows user to input database and table queried

usrnm = raw_input('Username: ')
table = 'paperdata' 
pswd = getpass.getpass('Password: ')

def dbsearch(query):

   # open a database connection
   # be sure to change the host IP address, username, password and database name to match your own
   connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

   # prepare a cursor object using cursor() method
   cursor = connection.cursor()

   # execute the SQL query using execute() method.
   cursor.execute('%s'%(query))

   #finds all rows outputted by query, prints them
   results = cursor.fetchall()
   print results
   #complete
   print 'Query Complete'

   # Close the cursor object
   cursor.close()
   connection.close()

#perform action: print all non-empty fields and concatenate into mysql string
def fetch(entries):
   query = 'SELECT '
   counter = 0
   for entry in entries:
      field = entry[0]
      text = entry[1].get()
      if len(entry) > 2:
         ltext = entry[2].get()
         rtext = entry[3].get()
      else:
         ltext = ''
         rtext = ''
      if ltext != '' or rtext != '' or text != '':
         if ltext == '' and rtext == '':
            text = '%' + text + '%'
            if counter == 0:
               #instead of * can create string from list created by loop
               #query += '%s FROM %s WHERE %s LIKE \'%s\'' %(field_string, table, vtext[field], text)
               query += '* FROM %s WHERE %s LIKE \'%s\'' %(table, vtext[field], text)
               counter += 1
            else:
               query += ' AND %s LIKE \'%s\'' %(vtext[field], text)
         elif ltext == '' and text == '':
            if counter == 0:
               query += '* FROM %s WHERE %s < %s' %(table, vtext[field], rtext)
               counter += 1
            else:
               query += ' AND %s < %s' %(vtext[field], rtext)
         elif rtext == '' and text == '':
            if counter == 0:
               query += '* FROM %s WHERE %s > %s' %(table, vtext[field], ltext)
               counter += 1
            else:
               query += ' AND %s > %s' %(vtext[field], ltext)
         elif text == '':
            if counter == 0:
               query += '* FROM %s WHERE (%s > %s AND %s < %s)' %(table, vtext[field], ltext, field, rtext)
               counter += 1
            else:
               query += ' AND (%s > %s AND %s < %s)' %(table, vtext[field], ltext, vtext[field], rtext)
#   execute other script to query mysql
   dbsearch(query)


#create form
def makeform(root, fields, mins, maxs):
   entries = []
#   for field in fields:
   for i in range(len(fields)):
      if i in [2,4,5,6,8]:
         row = Frame(root)
         lab = Label(row, width=15, text=fields[i], anchor='w')
         lab2 = Label(row, width=5, text=mins[i], anchor='e')
         lab3 = Label(row, width=5, text=maxs[i], anchor='e')
         ent = Entry(row)
         ent2 = Entry(row)
         ent3 = Entry(row)
         row.pack(side=TOP, fill=X, padx=5, pady=5)
         lab.pack(side=LEFT)
         ent3.pack(side=RIGHT, expand=YES, fill=X)
         lab3.pack(side=RIGHT)
         ent2.pack(side=RIGHT, expand=YES, fill=X)
         lab2.pack(side=RIGHT)
         ent.pack(side=RIGHT, expand=YES, fill=X)
         #creates entries list
         entries.append((fields[i], ent, ent2, ent3))
# create only one box for particular rows
      else:
         row = Frame(root)
         lab = Label(row, width=15, text=fields[i], anchor='w')
         ent = Entry(row)
         row.pack(side=TOP, fill=X, padx=5, pady=5)
         lab.pack(side=LEFT)
         ent.pack(side=RIGHT, expand=YES, fill=X)
         #creates entries list
         entries.append((fields[i], ent))

   return entries

if __name__ == '__main__':
   root = Tk()
   root.title('Paperdata Query')
   ents = makeform(root, fields, mins, maxs)
   root.bind('<Return>', (lambda event, e=ents: fetch(e)))
   b1 = Button(root, text='Show',
          command=(lambda e=ents: fetch(e)))
   b1.pack(side=LEFT, padx=5, pady=5)
   b2 = Button(root, text='Quit', command=root.quit)
   b2.pack(side=LEFT, padx=5, pady=5)
   root.mainloop()

