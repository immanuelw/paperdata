'''
Perform base conversions
Currently supports conversion between base 10 (decimal) and
base 60 (sexagesimal).
'''
from numpy import modf

def decimal_to_sexagesimal(decimal):
	'''
	convert decimal hours or degrees to sexagesimal

	Args:
		decimal (float): decimal number to be converted to sexagismal

	Returns:
		tuple:
			int: hours
			int: minutes
			float: seconds
		OR
		tuple:
			int: degrees
			int: arcminutes
			float: arcseconds
	'''
	fractional, integral = modf(decimal)
	min_fractional, minutes = modf(fractional * 60)
	seconds = min_fractional * 60.

	return integral.astype(int), minutes.astype(int), seconds

def sexagesimal_to_decimal(hd, minutes, seconds):
	'''
	convert sexagesimal hours or degrees to decimal
	Warning! Ensure each part has the correct sign
	e.g. -111d36m12s should be entered as (-111, -36, -12)

	Args:
		 hd (float): hours or degrees.
		 minutes (float): minutes or arcminutes
		 seconds (float): seconds or arcseconds

	Returns:
		 float: decimal hours or degrees
	'''
	return hd + minutes / 60. + seconds / 3600.
