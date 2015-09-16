rm -r paper/dev/
cp -r paper/data/ paper/dev/
sed -i 's/pdbi/dev/g' ~/paperdata/paper/dev/*.py
sed -i 's/data_db/dev_db/g' ~/paperdata/paper/dev/*.py
mv ~/paperdata/paper/dev/data_db.py ~/paperdata/paper/dev/dev_db.py
