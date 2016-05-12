'''
paper.site.monitor.scripts.run_app

runs monitor app

author | Immanuel Washington
'''
from paper.site.flask_app import monitor_app as app
from paper.site.monitor import views

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
