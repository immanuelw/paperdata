'''
scripts.paperdata.current_db

creates table depicting current summary of paperdata database

author | Immanuel Washington

Functions
---------
current_db | writes table summary of paperdata database into file
'''
import os
import prettytable
from sqlalchemy import func
from paper.data import dbi as pdbi

def current_db(s):
    '''
    writes table summary of paperdata database into file
    summary contraining information about julian days, hosts, etc.

    Parameters
    ----------
    s | object: session object
    '''
    obs_table = pdbi.Observation
    file_table = pdbi.File
    count = {}
    with open(os.path.join(ppdata.root_dir, 'config', 'table_descr.txt'), 'w') as df:
        count['era'] = s.query(obs_table.era, func.count(obs_table)).group_by(obs_table.era).all()
        count['julian day'] = s.query(obs_table.julian_day, func.count(obs_table)).group_by(obs_table.julian_day).all()
        count['host'] = s.query(file_table.host, func.count(file_table)).group_by(file_table.host).all()
        count['base path'] = s.query(file_table.base_path, func.count(file_table)).group_by(file_table.base_path).all()
        count['filetype'] = s.query(file_table.filetype, func.count(file_table)).group_by(file_table.filetype).all()

        for field, info in count.items():
            x = prettytable.PrettyTable((field.title(), 'Amount'))
            for values in info:
                x.add_row(values)
            stuff = x.get_string()
            df.write(stuff)

if __name__ == "__main__":
    dbi = pdbi.DataBaseInterface()
    with dbi.session_scope() as s:
        current_db(s)
