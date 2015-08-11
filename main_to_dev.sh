cp -r paperdata/ paperdev/
mv paperdev/scripts/paperdata_db.py paperdev/scripts/paperdev_db.py
sed -i 's/paperdev/paperdata/g' paperdev/*.py
sed -i 's/paperdev/paperdata/g' paperdev/scripts/*.py
