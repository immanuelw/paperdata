'''
paper.distiller.scripts.md5

updates md5sums of uv files without them

author | Immanuel Washington

Functions
---------
update_md5 -- updates md5sums of uv files
'''
from paper.data import file_data
from paper.distiller import dbi as ddbi

def update_md5(dbi):
	'''
	updates md5sums for all files without in database

	Parameters
	----------
	dbi | object: distiller database interface object
	'''
	with dbi.session_scope() as s:
		table = getattr(ddbi, 'File')
		FILEs = s.query(table).filter(getattr(table, 'md5sum') == None).all()
		for FILE in FILEs:
			dbi.set_entry(s, FILE, 'md5sum', file_data.calc_md5sum(getattr(FILE, 'host'), getattr(FILE, 'filename')))

if __name__ == '__main__':
	dbi = ddbi.DataBaseInterface()
	update_md5(dbi)
