paperdata v1.0
==============

Module for building, searching, and updating the PAPER database compression pipeline
------------------------------------------------------------------------------------

paper
>Main package for modules

-----------
DESCRIPTION
-----------

data
```
Contains various scripts which crawl certain hosts and build paperdata database.
Contains scripts which bridges other databases to the paperdata and populates it
```

dev
>test version of data module

ganglia
>Contains scripts to record the state of each host at any time in the ganglia database

site
>module & scripts for instantiation of websites for paperdata

calibration
>module & scripts for calibration of uv files

-------------
EXTRA MODULES
-------------

convert
>time conversions module

schema
>schema script function

-----
SETUP
-----

(1) git clone respository to user's root directory

(2) Copy all .cfg files to root paperdata directory
    replace fields within with correct credentials

(3) Copy settings.py file from paper/site/*/configs/settings.py to paper/site/*/
    replace SQLALCHEMY_DATABASE_URI with correct credentials

(4) [In virtualenv if possible] Run python setup.py install (develop if altering package)

(5) Further setup required if running docker container or rebuilding database
