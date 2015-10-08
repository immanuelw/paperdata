from paper.site.search import models
from paper.site.flask_app import search_db as db

# Insert the necessary default values into the database.

column_graph = getattr(models, 'GraphType')()
setattr(column_graph, 'name', 'Column')
db.session.add(column_graph)

line_graph = getattr(models, 'GraphType')()
setattr(line_graph, 'name', 'Line')
db.session.add(line_graph)

obs_file_graph = getattr(models, 'GraphType')()
setattr(obs_file_graph, 'name', 'Obs_File')
db.session.add(obs_file_graph)

db.session.flush() # So we don't violate the foreign key constraint when we add the data source.

obs_file_data_source = getattr(models, 'GraphDataSource')()
setattr(obs_file_data_source, 'name', 'Obs_File')
setattr(obs_file_data_source, 'graph_type', 'Obs_File')
db.session.add(obs_file_data_source)

db.session.commit()
