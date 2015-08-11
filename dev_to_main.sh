cp -r paperdev/ paperdata/
sed -i 's/paperdev/paperdata/g' paperdata/*.py
sed -i 's/paperdev/paperdata/g' paperdata/scripts/*.py
