#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import sys
import time
import os
import subprocess
import ddr_compress.dbi as ddbi
import json

### Script to Backup paper database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

import decimal
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)

def to_json(ser_data):
    json_dict = ser_data.__dict__
    try:
        del json_dict['_sa_instance_state']
    except(KeyError):
        return None

    return json_dict

def json_data(dbo, dump_objects):
    data = []
    with open(dbo, 'w') as f:
        for ser_data in dump_objects.all():
            data.append(to_json(ser_data))
        json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
    return None

def paperbackup(timestamp):

    backup_dir = os.path.join('/data4/paper/paperdistiller_backup', str(timestamp))
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)

    #Create separate files for each directory

    db1 = 'obs_{timestamp}.json'.format(timestamp=timestamp)
    dbo1 = os.path.join(backup_dir, db1)

    db2 = 'file_{timestamp}.json'.format(timestamp=timestamp)
    dbo2 = os.path.join(backup_dir, db2)

    db3 = 'log_{timestamp}.json'.format(timestamp=timestamp)
    dbo3 = os.path.join(backup_dir, db3)

    dbi = ddbi.DataBaseInterface()
    s = dbi.Session()

    OBS_table = getattr(ddbi, 'Observation')
    OBS_dump = s.query(OBS_table).order_by(getattr(OBS_table, 'julian_date').asc(), getattr(OBS_table, 'pol').asc())
    json_data(dbo1, OBS_dump)

    FILE_table = getattr(ddbi, 'File')
    FILE_dump = s.query(FILE_table).order_by(getattr(FILE_table, 'obsnum').asc(), getattr(FILE_table, 'filename').asc())
    json_data(dbo2, FILE_dump)

    LOG_table = getattr(ddbi, 'Log')
    LOG_dump = s.query(LOG_table).order_by(getattr(LOG_table, 'obsnum').asc(), getattr(LOG_table, 'timestamp').asc())
    json_data(dbo3, LOG_dump)

    s.close()
    print(timestamp)
    print('Table data backup saved')

    return None

if __name__ == '__main__':
    timestamp = time.strftime("%d-%m-%Y_%H:%M:%S")
    paperbackup(timestamp)
