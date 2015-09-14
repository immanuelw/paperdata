sed -i 's/pdbi/dev/g' ~/paperdata/dev/*.py
sed -i 's/data_db/dev_db/g' ~/paperdata/dev/*.py
mv ~/paperdata/dev/data_db.py ~/paperdata/dev/dev_db.py
