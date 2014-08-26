paperdata
=========

Files related to building, searching, and updating the PAPER database compression pipeline

Contains various scripts which crawl folio and build paperdata database.
Contains scripts which take in rows from paperdistiller database in order to populate paperdata database.

Important scripts:

	search_database.py:
		Used to search paperdata database. Can be incorporated into other scripts in order to do physics on list of files.

	update_database_from_paperdistiller.py:
		Used to update status of raw files and compressed files.

	move_uvcRRE.py:
		Used to automatically update database with new location when moving .uvcRRE files

	load_database.py:
		Used to fill paperdata with information directly from folio. Slow.

	load_database_from_paperdistiller:
		Used to fill paperdata with information from paperdistiller database. Much faster than crawling though folio.

	check_raw.py:
		Used to check for any raw file if corresponding compressed file exists

	compression_check.py:
		Used to update database to see if taping can be done through checking if all the raw and compressed files
		for any particular Julian Day all exist.

	auto_compress.py:
		Used to run compression pipeline on raw files.

	delete_raw_files.py:
		Used to delete files from folio which have been written to tape

	rename.py:
		Used to rename files which have lost their names due to accidental deletion of pot0

	backup_paperdata.py:
		Used to backup the paperdata database, saving to a .csv file to be easily reloaded.
