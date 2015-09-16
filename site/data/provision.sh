#!/usr/bin/env bash
echo ---provision.sh---

#Set a system-wide environment variable for the database's location.
echo 'export DATABASE_URL="postgres:///postgres"' >> ~/.profile

cat ~/.profile

export DATABASE_URL=postgres:///postgres

sudo apt-get update
sudo apt-get install -y python-virtualenv python3-dev libpq-dev postgresql libmysqlclient-dev mysql-client

cd /mnt/paper/site/data

sudo pip install --upgrade virtualenv
virtualenv --python=/usr/bin/python3.4 flask
source flask/bin/activate
pip install flask requests Flask-SQLAlchemy Flask-Migrate Flask-Login requests-futures psycopg2 mysqlclient

cd /mnt/paper/
python3.4 setup.py install

cd /mnt/paper/site/data

sudo -u postgres createuser vagrant

python3.4 -m app.manage db upgrade

chmod +x run_app.py
./run_app.py
