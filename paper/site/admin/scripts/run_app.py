'''
paper.site.admin.scripts.run_app

runs admin app

author | Immanuel Washington
'''
from paper.site.flask_app import admin_app as app
from paper.site.admin import views, models, comments, users

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
