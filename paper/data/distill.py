'''
paper.data.distill

adds files to paperdistiller database

author | Immanuel Washington

Functions
---------
add_files | adds files to paperdistiller database after pulling some relevant information
'''
from __future__ import print_function
import os
import socket
import numpy as n
import paper as ppdata
from paper.data import uv_data
from paper.distiller import dbi as ddbi

def add_files(source_paths):
    '''
    adds files to paperdistiller database

    Parameters
    ----------
    source_paths | list[str]: list of file paths
    '''
    #connect to the database
    dbi = ddbi.DataBaseInterface()

    #check that all files exist
    for source_path in source_paths:
        assert(source_path.startswith('/'))
        assert(os.path.exists(source_path))

    #now run through all the files and build the relevant information for the db
    # get the pols
    pols = [ppdata.file_to_pol(source_path) for source_path in source_paths]
    jds = n.array([str(ppdata.file_to_jd(source_path)) for source_path in source_paths])
    nights = list(set(jds.astype(n.int)))

    jds_onepol = n.sort([jd for i, jd in enumerate(jds) if pols[i] == pols[0] and jd.astype(int) == nights[0]])
    djd = n.mean(n.diff(jds_onepol))
    print('setting length to ', djd, ' days')

    pols = list(set(pols))#these are the pols I have to iterate over
    print('found the following pols', pols)
    print('found the following nights', nights)

    host = socket.gethostname()

    for night in nights:
        print('adding night', night)
        obsinfo = []
        nightfiles = [source_path for source_path in source_paths if int(ppdata.file_to_jd(source_path)) == night]
        print(len(nightfiles))
        for pol in pols:
            #filter off all pols but the one I'm currently working on
            paths = sorted(source_path for source_path in nightfiles if ppdata.file_to_pol(source_path) == pol)
            with dbi.session_scope() as s:
                for source_path in paths:
                    try:
                        s.query(ddbi.Observation).get(uv_data.jdpol_to_obsnum(ppdata.file_to_jd(source_path), ppdata.file_to_pol(source_path), djd))
                        print(source_path, 'found in db, skipping')
                    except:
                        obsinfo.append({'julian_date': ppdata.file_to_jd(source_path),
                                        'pol': ppdata.file_to_pol(source_path),
                                        'host': host,
                                        'filename': source_path,
                                        'length': djd}) #note the db likes jd for all time units

        for i, obs in enumerate(obsinfo):
            if i != 0:
                if n.abs(obsinfo[i - 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
                    obsinfo[i].update({'neighbor_low': obsinfo[i - 1]['julian_date']})
            if i != len(obsinfo) - 1:
                if n.abs(obsinfo[i + 1]['julian_date'] - obs['julian_date']) < (djd * 1.2):
                    obsinfo[i].update({'neighbor_high': obsinfo[i + 1]['julian_date']})

        print('adding {obs_len} observations to the still db'.format(obs_len=len(obsinfo)))

        try:
            with dbi.session_scope() as s:
                for info_dict in obsinfo:
                    s.add(ddbi.Observation(**info_dict))
        except:
            print('problem!')
    print('done')

if __name__ == '__main__':
    print('This is just a module')
