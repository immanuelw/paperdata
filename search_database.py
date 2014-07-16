#!/usr/bin/python
# -*- coding: utf-8 -*-
# Search data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
from Tkinter import *
import os

#initalize lists to create gui with

fields = 'file name', 'location', 'antennas', 'obsnum', 'Julian Day', 'Julian Date', 'polarization', 'Length of data', 'raw location', 'Calibrate file', 'file size'

mins = 'N/A', 'N/A', 'min', 'min', 'min', 'min', 'N/A', 'min', 'N/A', 'N/A', 'N/A'

maxs = 'N/A', 'N/A', 'max', 'max', 'max', 'max', 'N/A', 'max', 'N/A', 'N/A', 'N/A'

vtext = {fields[0]:'filename', fields[1]:'location', fields[2]:'antennas', fields[3]:'obsnum', fields[4]:'julian_day', fields[5]:'julian_date', fields[6]:'polarization', fields[7]:'data_length', fields[8]:'raw_location', fields[9]:'cal_location', fields[10]:'file_size'}

#allows user to input table queried
table = raw_input('Search table named:') 

#perform action: print all non-empty fields and concatenate into mysql string
def fetch(entries):
   query = 'SELECT '
   counter = 0
   for entry in entries:
      field = entry[0]
      text  = entry[1].get()
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
               query += ' AND (%s > %s AND %s < %s)' %(table, vtext[field], ltext,vtext[field], rtext)
#   execute other script to query mysql
   #os.system('python /data2/home/immwa/scripts/paper/pysql.py "%s"' %(query))
   dbsearch(query)


#create form
def makeform(root, fields, mins, maxs):
   entries = []
#   for field in fields:
   for i in range(len(fields)):
      if i in [2,3,4,5,7]:
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
   root.title('Folio MySQL query')
   ents = makeform(root, fields, mins, maxs)
   root.bind('<Return>', (lambda event, e=ents: fetch(e)))
   b1 = Button(root, text='Show',
          command=(lambda e=ents: fetch(e)))
   b1.pack(side=LEFT, padx=5, pady=5)
   b2 = Button(root, text='Quit', command=root.quit)
   b2.pack(side=LEFT, padx=5, pady=5)
   root.mainloop()

def dbsearch(query)
   datab = raw_input('Database:')
   pswd = getpass.getpass('Password:')

   # open a database connection
   # be sure to change the host IP address, username, password and database name to match your own
   connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

   # prepare a cursor object using cursor() method
   cursor = connection.cursor()

   # execute the SQL query using execute() method.
   cursor.execute('%s'%(query))

   #finds all rows outputted by query, prints them
   results = cursor.fetchall()
   print results
   #complete
   print 'Query Complete'

   # close the cursor object
   cursor.close()

   #save changes to database
   connection.commit()

   # close the connection
   connection.close()

   # exit the program
   sys.exit()

