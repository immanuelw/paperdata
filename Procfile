web: gunicorn --pythonpath paper.site.search.scripts run_app:app --log-file=-
init: python -m search.manage db init
migrate: python -m search.manage db migrate
upgrade: python -m search.manage db upgrade
default_values: python -m scripts.insert_default_db_values
