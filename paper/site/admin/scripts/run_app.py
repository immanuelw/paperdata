#!flask/bin/python

from paper.site.admin.flask_app import app
from paper.site.admin import views, models, comments, users

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0')
