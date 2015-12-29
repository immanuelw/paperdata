'''
paper.site.search.scripts.run_app

runs search app

author | Immanuel Washington
'''
from paper.site.flask_app import search_app as app
from paper.site.search import views, models

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
