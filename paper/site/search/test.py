from paper.site import db_utils
start_utc = 2456000
end_utc = 2456400
response = db_utils.query(database='paperdata', table='Observation',
							field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc)),
							sort_tuples=(('time_start', 'asc'),))
for i in response:
	print(i.to_dict())
