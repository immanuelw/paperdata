'''
heralive.search.run_heralive

runs search app

author | Immanuel Washington
'''
from flask_app import search_app as app
import views

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
