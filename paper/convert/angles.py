'''
Perform various angle related transformations
Transform between different notations for angles:
Degrees, radians and hours.
'''
from numpy import radians, degrees

def hours_to_degrees(angle):
	'''
	converts decimal hours to degrees

	Parameters
	----------
	hours | float: angle in decimal hours

	Returns
	-------
	float: angle in degrees
	'''
	return angle * 15.

def hours_to_radians(angle):
	'''
	converts decimal hours to radians

	Parameters
	----------
	hours(float): angle in decimal hours

	Returns
	-------
	float: angle in radians
	'''
	return radians(hours_to_degrees(angle))

def degrees_to_hours(angle):
	'''
	converts degrees to decimal hours

	Parameters
	----------
	angle | float: angle in degrees

	Returns
	-------
	float: angle in decimal hours
	'''
	return angle / 15.

def radians_to_hours(angle):
	'''
	converts degrees to decimal hours

	Parameters
	----------
	angle | float: angle in degrees

	Returns
	-------
	float: angle in decimal hours
	'''
	return degrees_to_hours(degrees(angle))
