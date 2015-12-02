web: gunicorn --pythonpath paper/site/search/scripts run_app:app --log-file=-
init: python -m paper.site.search.manage db init
migrate: python -m paper.site.search.manage db migrate
upgrade: python -m paper.site.search.manage db upgrade
