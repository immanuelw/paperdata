from app import models
from app.flask_app import db

# Insert the necessary default values into the database.

column_graph = getattr(models, 'Graph_Type')()
setattr(column_graph, 'name', 'Column')
db.session.add(column_graph)

line_graph = getattr(models, 'Graph_Type')()
setattr(line_graph, 'name', 'Line')
db.session.add(line_graph)

obs_file_graph = getattr(models, 'Graph_Type')()
setattr(obs_file_graph, 'name', 'Obs_File')
db.session.add(obs_file_graph)

db.session.flush() # So we don't violate the foreign key constraint when we add the data source.

obs_file_data_source = getattr(models, 'Graph_Data_Source')()
setattr(obs_file_data_source, 'name', 'Obs_File')
setattr(obs_file_data_source, 'graph_type', 'Obs_File')
db.session.add(obs_file_data_source)

db.session.commit()
