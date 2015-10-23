from paper.data import dbi as pdbi
from sqlalchemy import func
dbi = pdbi.DataBaseInterface()
with dbi.session_scope() as s:
	obs_table = pdbi.Observation
	response = s.query(obs_table.polarization, obs_table.era_type, func.sum(obs_table.length) * 24, func.count(obs_table)).group_by(obs_table.polarization, obs_table.era_type).all()
	print(response)
	file_table = pdbi.File
	response = s.query(file_table.host, file_table.filetype, func.count(file_table)).group_by(file_table.host, file_table.filetype).all()
	print(response)
