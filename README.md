paperdata
=========

Files related to building, searching, and updating the PAPER database compression pipeline

Contains various scripts which crawl folio and build paperdata database.
Contains scripts which take in rows from paperdistiller database in order to populate paperdata database.

Important scripts:

	search_database.py:
		Used to search paperdata database. Can be incorporated into other scripts in order to do physics on list of files.

	move_uvcRRE.py:
		Used to automatically update database with new location when moving .uvcRRE files

	load_database_from_folio.py:
		Used to fill paperdata with information directly from folio. Slow.

	load_database_from_paperdistiller.py:
		Used to fill paperdata with information from paperdistiller database. Much faster than crawling though folio.

	load_raw_from_folio.py:
		Used to load raw data information into paperdata without a corresponding compressed file

	check_raw.py:
		Used to check for any raw file if corresponding compressed file exists

	compression_check.py:
		Used to update database to check if a compressed file exists in indicated location.

	tape_check.py:
		Used to check if every file in a particular Julian Day has a compressed file and thusly can be written to tape.

	auto_compress.py:
		Used to run compression pipeline on raw files. INCOMPLETE

	delete_raw_files.py:
		Used to delete files from folio which have been written to tape and update paperdata to reflect that.

	rename.py:
		Used to rename files which have lost their names due to accidental deletion of pot0.

	backup_paperdata.py:
		Used to backup the paperdata database, saving to a .csv file to be easily reloaded.

	load_database_from_backup.py:
		Used to refill paperdata after backup due to error or changed fields.

	backup_paperdistiller.py:
		Used to backup the several tables of paperdistiller -- Used mostly for testing purposes.

	log_errors_from_folio.py:
		Used to check for unaccessible/incomplete .uvcRRE files within folio.

	make_paperdata_table.py:
		Used to create the base of the paperdata table -- includes field names and types.

	describe_paperdata.py:
		Used to generate a list of field names and types of paperdata -- generates paperdata_schema base.
		Do not run unless paperdata table has been rebuilt recently.

	paperdata_schema:
		Description of each field in paperdata.

	clear_paperdistiller.py:
		Used to clear information from the paperdistiller database -- Used primarily in testing.
