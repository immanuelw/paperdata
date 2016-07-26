## paperdata

*Module for building, searching, and updating the PAPER database compression pipeline*

-----
SETUP
-----

(1) git clone respository

(2) Rename all .cfg.test files .cfg inside config directory

	replace fields within with correct credentials

(3) [In virtualenv if possible] Run python setup.py install (develop if altering package)

### Install

```js
python setup.py install
```

### Dev Install
```js
python setup.py develop
```

(4) Further setup required if running docker container or rebuilding database

-----------
DESCRIPTION
-----------

## paper
```
Main package for modules
```

### data
```
Contains modules which directly interact with the paperdata database
```

### distiller
```
Contains modules which directly interact with the paperdistiller database
```

### ganglia
```
Contains modules to record the state of each host at any time in the ganglia database
```

### calibrate
```
module & scripts for calibration of uv files
NOW DEFUNCT
```

## heralive
```
module & scripts for instantiation of websites for paperdata
```

-------------
EXAMPLE QUERY
-------------
*Example of how to get all compressed files in database in a certain range julian days and change field is_tapeable to True*
```js
from paper.data import dbi as pdbi

dbi = pdbi.DataBaseInterface() <!--instantiate DBI object-->
with dbi.session_scope() as s: <!--instantiate session object as context manager-->
	FILE_query = s.query(pdbi.File).join(pdbi.Observation) <!--grabs base query object and joins table-->
	<!--filters query to look for particular range of dates and a file type-->
	filtered_query = FILE_query.filter(pdbi.File.filetype == 'uvcRRE')\
							   .filter(pdbi.Observation.julian_day >= 2455903)
							   .filter(pdbi.Observation.julian_day <= 2456036)
	FILEs = filtered_query.all() <!--gets generator of all FILE objects-->
	for FILE in FILEs:
		FILE.is_tapeable = True
	<!--automatically commits to database upon finishing due to context manager-->
```

-------
LICENSE
-------
```
GPL. Inside LICENSE file
```
