#!/usr/bin/env bash
echo ---provision.sh---

#Set a system-wide environment variable for the database's location.
echo 'export DATABASE_URL="postgres:///postgres"' >> ~/.profile

cat ~/.profile

export DATABASE_URL=postgres:///postgres

sudo apt-get update
sudo apt-get install -y python-virtualenv python3-dev libpq-dev postgresql

cd /mnt/eorlive_prototype

sudo virtualenv --python=/usr/bin/python3.4 flask
source flask/bin/activate
pip install flask requests Flask-SQLAlchemy Flask-Migrate Flask-Login requests-futures psycopg2

sudo -u postgres createuser vagrant

python3.4 -m app.manage db upgrade

chmod +x run_app.py
./run_app.py
