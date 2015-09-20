#!flask/bin/python

from paper.site.flask_app import search_app as app
from paper.site.search import views, models, sets, comments, data_sources, filters, users

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')
