#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
from __future__ import print_function
import sys
import time
import os
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import dbi as dev
import json
import time

### Script to Backup paper database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

import decimal
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)

def json_data(dbo, dump_objects):
    data = []
    with open(dbo, 'w') as f:
        for ser_data in dump_objects.all():
            data.append(ser_data.to_json())
        json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
    return None

def paperbackup(timestamp):

    backup_dir = os.path.join('/data4/paper/paper_backup', str(timestamp))
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)

    #Create separate files for each directory

    db1 = 'obs_{timestamp}.json'.format(timestamp=timestamp)
    dbo1 = os.path.join(backup_dir, db1)
    print(dbo1)

    db2 = 'file_{timestamp}.json'.format(timestamp=timestamp)
    dbo2 = os.path.join(backup_dir, db2)
    print(dbo2)

    #db3 = 'feed_{timestamp}.json'.format(timestamp=timestamp)
    #dbo3 = os.path.join(backup_dir, db3)
    #print(dbo3)

    db4 = 'log_{timestamp}.json'.format(timestamp=timestamp)
    dbo4 = os.path.join(backup_dir, db4)
    print(dbo4)

    dbi = dev.DataBaseInterface()
    s = dbi.Session()

    OBS_table = getattr(dev, 'Observation')
    OBS_dump = s.query(OBS_table).order_by(getattr(OBS_table, 'julian_date').asc(), getattr(OBS_table, 'polarization').asc())
    json_data(dbo1, OBS_dump)

    FILE_table = getattr(dev, 'File')
    FILE_dump = s.query(FILE_table).order_by(getattr(FILE_table, 'obsnum').asc(), getattr(FILE_table, 'filename').asc())
    json_data(dbo2, FILE_dump)

    #FEED_table = getattr(dev, 'File')
    #FEED_dump = s.query(FEED_table).order_by(getattr(FEED_table, 'julian_day').asc(), getattr(FEED_table, 'filename').asc())
    #json_data(dbo3, FEED_dump)

    LOG_table = getattr(dev, 'Log')
    LOG_dump = s.query(LOG_table).order_by(getattr(LOG_table, 'timestamp').asc(), getattr(LOG_table, 'action').asc())
    json_data(dbo4, LOG_dump)

    s.close()
    print('Table data backup saved')

    return None

def email_backup(backup_file):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    #Next, log in to the server
    server.login('paperfeed.paper@gmail.com', 'papercomesfrom1tree')

    msg = MIMEMultipart()
    msg['Subject'] = 'PAPERDATA TABLE BACKUP'
    msg['From'] = 'paperfeed.paper@gmail.com'
    msg['To'] = 'paperfeed.paper@gmail.com'

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(open(backup_file, 'rb').read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=backup_file)
    msg.attach(part)

    #Send the mail
    server.sendmail('paperfeed.paper@gmail.com', 'paperfeed.paper@gmail.com', msg.as_string())

    server.quit()

    return None

if __name__ == '__main__':
    timestamp = int(time.time())

    paperbackup(timestamp)
    #backup_file = '/data4/paper/paper_backup/{timestamp}/paper_backup.psv'.format(timestamp=timestamp)
    #email_backup(backup_file)
