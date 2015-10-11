'''
paper.site.search.scripts.insert_default_db_values

adds default values into database

author | Immanuel Washington
'''
from paper.site.search import models
from paper.site.flask_app import search_db as db

column_graph = models.GraphType(name='Column')
db.session.add(column_graph)

line_graph = models.GraphType(name='Line')
db.session.add(line_graph)

obs_file_graph = models.GraphType(name='Obs_File')
db.session.add(obs_file_graph)

db.session.flush() # So we don't violate the foreign key constraint when we add the data source.

obs_file_data_source = models.GraphDataSource(name='Obs_File', graph_type='Obs_File')
db.session.add(obs_file_data_source)

db.session.commit()
