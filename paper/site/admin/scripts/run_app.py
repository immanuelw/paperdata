#!flask/bin/python

from paper.site.search.flask_app import app
from paper.site.search import views, models, sets, comments, data_sources, filters, users

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')
