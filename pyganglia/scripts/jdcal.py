import math

MJD_0 = 2400000.5
MJD_JD2000 = 51544.5

def fpart(x):
	"""Return fractional part of given number."""
	return math.modf(x)[0]

def ipart(x):
	"""Return integer part of given number."""
	return math.modf(x)[1]

def is_leap(year):
	"""Leap year or not in the Gregorian calendar."""
	x = math.fmod(year, 4)
	y = math.fmod(year, 100)
	z = math.fmod(year, 400)
	# Divisible by 4 and,
	# either not divisible by 100 or divisible by 400.
	return not x and (y or not z)

def gcal2jd(year, month, day, hour, minute, second):
	year = int(year)
	month = int(month)
	day = int(day)
	hour = int(hour)
	minute = int(minute)
	second = int(second)
	a = ipart((month - 14) / 12.0)
	jd = ipart((1461 * (year + 4800 + a)) / 4.0)
	jd += ipart((367 * (month - 2 - 12 * a)) / 12.0)
	x = ipart((year + 4900 + a) / 100.0)
	jd -= ipart((3 * x) / 4.0)
	jd += day - 2432075.5 # was 32075; add 2400000.5
	jd -= 0.5 # 0 hours; above JD is for midday, switch to midnight.
	jd += (hour + (minute + (second) / 60.0) / 60.0) / 24.0
	return MJD_0 + jd
