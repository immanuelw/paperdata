'''
paper.data

author | Immanuel Washington

Modules
-------
dbi | paperdata database interface
data_db | paperdata database tables schema
file_data | shared file metadata parsing functions
uv_data | shared uv file data parsing functions

Scripts
-------
add_files | add files and observations to paperdata database
backup_db | paperdata database backup
create_db | instantiate paperdata tables
current_db | create file containing table describing current state of database
delete_files | delete files contained in database and update entries
distill_bridge | pull information about files from paperdistiller database into paperdata database
distill_files | adds files to paperdistiller database
feed_bridge | pulls information about files from feed table into paperdistiller database
feed_files | adds files to feed table
move_files | moves files contained in database and update entries
refresh_db | refreshes database by removing and updating entries to be correct to current status of hosts
reload_db | crawls folio for all uv files in case of catastrophic database failure
frestore_db | restores database from backup
schema_db | schema file generation
test_db | tests most scripts for pipeline
test_mod | runs tests for doctests of paper pipeline
'''
