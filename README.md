## paperdata

*Module for building, searching, and updating the PAPER database compression pipeline*

-----
SETUP
-----

(1) git clone respository

(2) Rename all .cfg.test files .cfg inside config directory

replace fields within with correct credentials

(3) [In virtualenv if possible] Run python setup.py install (develop if altering package)

## Install

```js
python setup.py install
```

## Dev Install
```js
python setup.py develop
```

(4) Further setup required if running docker container or rebuilding database

-----------
DESCRIPTION
-----------

# paper
```
Main package for modules
```

# data
```
Contains modules which directly interact with the paperdata database
```

# ganglia
```
Contains modules to record the state of each host at any time in the ganglia database
```

# heralive
```
module & scripts for instantiation of websites for paperdata
```

# calibration
```
module & scripts for calibration of uv files
```
