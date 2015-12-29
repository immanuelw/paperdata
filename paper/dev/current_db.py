#!/usr/bin/env python
import os
import prettytable
import dbi as dev
from sqlalchemy import func
from sqlalchemy.sql import label

#script to show state of paper

def main():
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    current_FILEs = s.query(dev.File).all()
    s.close()
    current = tuple(((getattr(FILE, 'era'), getattr(FILE, 'julian_day'), getattr(FILE, 'host'),
                    getattr(FILE, 'path'), getattr(FILE, 'filetype'), getattr(FILE, 'source_host')) for FILE in FILEs))

    count = {}
    for entry in current:
        if entry in count.keys():
            count[entry] += 1
        else:
            count[entry] = 1
    out = []
    for key, value in count.items():
        out.append(key + (value,))
        

    x = PrettyTable(['Era', 'Julian Day', 'Host', 'Path', 'Type', 'Source Host', 'Amount'])
    for line in out:
        x.add_row(line)
    stuff = x.get_string()
    with open(os.path.expanduser('~/src/table_descr.txt'), 'wb') as df:
        df.write(stuff)

if __name__ == "__main__":
    main()

