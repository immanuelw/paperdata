cp -r paperdev_dbi/ paperdata_dbi/
mv paperdata_dbi/scripts/paperdev_db.py paperdata_dbi/scripts/paperdata_db.py
sed -i 's/paperdev/paperdata/g' paperdata_dbi/*.py
sed -i 's/paperdev/paperdata/g' paperdata_dbi/scripts/*.py
