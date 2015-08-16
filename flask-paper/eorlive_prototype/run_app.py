#!flask/bin/python

from app.flask_app import app
from app import views, models, sets, comments, data_sources, filters, users

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')
