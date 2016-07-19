'''
paper.distiller.scripts.update_md5

updates md5sums of uv files without them

author | Immanuel Washington

Functions
---------
update_md5 | updates md5sums of uv files
'''
from paper.data import file_data
from paper.distiller import dbi as ddbi

def update_md5(s):
    '''
    updates md5sums for all files without in database

    Parameters
    ----------
    s | object: session object
    '''
    table = ddbi.File
    FILEs = s.query(table).filter_by(md5sum=None).all()
    for FILE in FILEs:
        FILE.md5sum = file_data.calc_md5sum(FILE.host, FILE.filename)

if __name__ == '__main__':
    dbi = ddbi.DataBaseInterface()
    with dbi.session_scope() as s:
        update_md5(s)
