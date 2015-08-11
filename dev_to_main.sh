cp -r paperdev/ paperdata/
mv paperdata/scripts/paperdev_db.py paperdata/scripts/paperdata_db.py
sed -i 's/paperdev/paperdata/g' paperdata/*.py
sed -i 's/paperdev/paperdata/g' paperdata/scripts/*.py
