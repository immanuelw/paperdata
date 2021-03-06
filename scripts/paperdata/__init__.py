'''
scripts.paperdata

author | Immanuel Washington

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
restore_db | restores database from backup
schema_db | schema file generation
'''
