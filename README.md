paperdata v1.0
==============

Files related to building, searching, and updating the PAPER database compression pipeline

Contains various scripts which crawl folio and build paperdata database.
Contains scripts which take in rows from paperdistiller database in order to populate paperdata database.

Important scripts:

	paperdataDB module:
		Used to search paperdata database. Can be incorporated into other scripts in order to do physics on list of files.

	search_paperdata.py:
		Used in conjunction with paperdataDB module to create a GUI which can search the paperdata database as well
		as output an SQL string.

	move_data.py:
		Automatically updates database with new location when moving .uvcRRE or .uv files
		MUST USE THIS WHEN MOVING ANY FILES IN THE DATABASE

	load_paperdata.py:
		Loads raw and compressed data information into paperdata from files on folio.

	load_paper*.py:
		Loads relevant information into paper* table in paperdata database from files on folio.

	tape_check.py:
		Checks if every file in a particular Julian Day has a compressed file and thusly can be written to tape.

	delete_raw_files.py:
		Delete files from folio which have been written to tape and update paperdata to reflect that.

	load_backup.py:
		Refill paperdata table after backup due to error or changed fields.

	make_paper_db_table.py:
		Creates the base of any table in the paperdata database -- includes field names and types.

	consolidate_paperdata.py:
		Runs through paperdata, joining matching .uv and .uvcRRE files and creating a new paperdata table by writing to a .psv file.
		--Still experimental, but functioning and eliminating all disjoint entries


	***DAEMONS***
	***All daemons are perpetually running versions of their respective python script***

	paperbridge.py & bridge_daemon.py:
		Fills paperdata with information from completed files in the paperdistiller database.

	paperjunk.py & junk_daemon.py:
		Pulls files from pot1 and updates paperjunk table with new location.

	paperrename.py & rename_daemon.py:
		Rebuilds .uv files moved from pot1 and existing in the paperjunk table. Loads to paperfeed table if entire julian day exists.
		Possible hub location for all .uv files before entering paperfeed table.

	paperfeed.py & feed_daemon.py:
		Pulls data from paperrename table to generate table full of .uv files waiting to be compressed.
		Moves .uv files to centralized location -- sends to paperdistiller to be compressed.

	paperbackup.py & backup_daemon.py:
		Backups the entire paperdata database, loading all into a created folder labeled with time and data
		-- separated .psv files by table.

	papermd5.py & md5_daemon.py:
		Creates md5sums for all files without in paperdistiller for quicker loading into paperdata.

	***DATABASE DESCRIPTION***

	current_paperdata.py:
		Creates table describing the current status of data in the database -- location, amount raw and compressed, julian_day. 
		-- Table located in table_descr.txt

	table_descr.txt:
		Table describing the current status of data in the database -- location, amount raw and compressed, julian_day.

	db_paperdata_schema:
		Description of each field in paperdata.


	***TESTING***

	backup_paperdata.py:
		Backups the paperdata table, saving to a .psv file to be easily reloaded. Used mostly for testing.

	paperclear.py:
		Clears information from the paperdata database -- Used exclusively for rebuliding paperdata

	distillerbackup.py:
		Backups the several tables of paperdistiller -- Used mostly for testing purposes.

	distillerclear.py:
		Clears information from the paperdistiller database -- Used primarily in testing.
