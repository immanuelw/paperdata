# Standard library dependencies
import datetime
import math
import re

# Formatting conversions:
MJD_0 = 2400000.5
MJD_JD2000 = 51544.5

def fpart(x):
	'''Return fractional part of given number.'''
	return math.modf(x)[0]

def ipart(x):
	'''Return integer part of given number.'''
	return math.modf(x)[1]

def is_leap(year):
	'''Leap year or not in the Gregorian calendar.'''
	x = math.fmod(year, 4)
	y = math.fmod(year, 100)
	z = math.fmod(year, 400)

	# Divisible by 4 and,
	# either not divisible by 100 or divisible by 400.
	return not x and (y or not z)

def gcal2jd(year, month, day, hour=None, minute=None, second=None):
	year = int(year)
	month = int(month)
	day = int(day)

	a = ipart((month - 14) / 12.0)
	jd = ipart((1461 * (year + 4800 + a)) / 4.0)
	jd += ipart((367 * (month - 2 - 12 * a)) / 12.0)
	x = ipart((year + 4900 + a) / 100.0)
	jd -= ipart((3 * x) / 4.0)
	jd += day - 2432075.5  # was 32075; add 2400000.5

	jd -= 0.5  # 0 hours; above JD is for midday, switch to midnight.
	#add to jd for hms 
	if hour:
		jd += hour / (24.0)
	if minute:
		jd += minute / (24.0 * 60)
	if second:
		jd += second / (24.0 * 60 * 60)

	return MJD_0, round(jd, 5)

def jd2gcal(jd1, jd2):
	from math import modf

	jd1_f, jd1_i = modf(jd1)
	jd2_f, jd2_i = modf(jd2)

	jd_i = jd1_i + jd2_i

	f = jd1_f + jd2_f

	# Set JD to noon of the current date. Fractional part is the
	# fraction from midnight of the current date.
	if -0.5 < f < 0.5:
		f += 0.5
	elif f >= 0.5:
		jd_i += 1
		f -= 0.5
	elif f <= -0.5:
		jd_i -= 1
		f += 1.5

	l = jd_i + 68569
	n = ipart((4 * l) / 146097.0)
	l -= ipart(((146097 * n) + 3) / 4.0)
	i = ipart((4000 * (l + 1)) / 1461001)
	l -= ipart((1461 * i) / 4.0) - 31
	j = ipart((80 * l) / 2447.0)
	day = l - ipart((2447 * j) / 80.0)
	l = ipart(j / 11.0)
	month = j + 2 - (12 * l)
	year = 100 * (n - 49) + i + l

	return int(year), int(month), int(day), f

def jcal2jd(year, month, day):
	year = int(year)
	month = int(month)
	day = int(day)

	jd = 367 * year
	x = ipart((month - 9) / 7.0)
	jd -= ipart((7 * (year + 5001 + x)) / 4.0)
	jd += ipart((275 * month) / 9.0)
	jd += day
	jd += 1729777 - 2400000.5  # Return 240000.5 as first part of JD.

	jd -= 0.5  # Convert midday to midnight.

	return MJD_0, jd

def jd2jcal(jd1, jd2):
	from math import modf

	jd1_f, jd1_i = modf(jd1)
	jd2_f, jd2_i = modf(jd2)

	jd_i = jd1_i + jd2_i

	f = jd1_f + jd2_f

	# Set JD to noon of the current date. Fractional part is the
	# fraction from midnight of the current date.
	if -0.5 < f < 0.5:
		f += 0.5
	elif f >= 0.5:
		jd_i += 1
		f -= 0.5
	elif f <= -0.5:
		jd_i -= 1
		f += 1.5

	j = jd_i + 1402.0
	k = ipart((j - 1) / 1461.0)
	l = j - (1461.0 * k)
	n = ipart((l - 1) / 365.0) - ipart(l / 1461.0)
	i = l - (365.0 * n) + 30.0
	j = ipart((80.0 * i) / 2447.0)
	day = i - ipart((2447.0 * j) / 80.0)
	i = ipart(j / 11.0)
	month = j + 2 - (12.0 * i)
	year = (4 * k) + n + i - 4716.0

	return int(year), int(month), int(day), f
def dec2sex(deci):
	(hfrac, hd) = math.modf(deci)
	(min_frac, m) = math.modf(hfrac * 60)
	s = min_frac * 60.
	return (int(hd), int(m), s)

def sex2dec(hd, min, sec):
	return float(hd) + min/60.0 + sec/3600.0

def datetime2decHours(time):
	return time.hour + time.minute/60.0 + time.second/3600.0 + time.microsecond/3600000000.0

def string2hours(string):
	string = string.strip() # trim leading/trailing whitespace

	div = '[|:|\s]' # allowable field delimiters "|", ":", whitespace
	
	# (optional +/-) degrees + min + decimal seconds
	sdec = '([+-]?)(\d{1,3})' + div + '(\d{1,2})' + div + '(\d{1,2}\.?\d+?)'
	
	co_re= re.compile(sdec)
	co_search= co_re.search(string)
	
	if co_search is None:
		raise ValueError("Invalid input string: '%s'" % string)
	elems = co_search.groups()

	plusminus = elems[0]
	hours = float(elems[1])
	minutes = float(elems[2])
	seconds = float(elems[3])
	
	# Check for nonsense values
	if hours > 24.0:
		raise ValueError("Hour value must be < 24.")
	if minutes >= 60.0:
		raise ValueError("Minute value must be < 60.")
	if seconds >= 60.0:
		raise ValueError("Second value must be < 60.")
		
	# Convert dec
	decHours = hours + minutes/60.0 + seconds/3600.0
	
	if plusminus is "-":
		decHours = -1.0 * decHours
	
	return decHours
	
def string2deg(string):
	string = string.strip() # trim leading/trailing whitespace

	div = '[|:|\s]' # allowable field delimiters "|", ":", whitespace
	
	# (optional +/-) degrees + min + decimal seconds
	sdec = '([+-]?)(\d{1,3})' + div + '(\d{1,2})' + div + '(\d{1,2}\.?\d+?)'
	
	co_re= re.compile(sdec)
	co_search= co_re.search(string)
	
	if co_search is None:
		raise ValueError("Invalid input string: %s" % string)
	elems = co_search.groups()

	plusminus = elems[0]
	degrees = float(elems[1])
	arcminutes = float(elems[2])
	arcseconds = float(elems[3])
	
	# Check for nonsense values
	if degrees > 90.0:
		raise ValueError("Degree value must be <= 90.")
	if arcminutes >= 60.0:
		raise ValueError("Arcminute value must be < 60.")
	if arcseconds >= 60.0:
		raise ValueError("Arcsecond value must be < 60 (was %f)." % arcseconds)
		
	# Convert dec
	decDegrees = degrees + arcminutes/60.0 + arcseconds/3600.0
	
	if plusminus is "-":
		decDegrees = -1.0 * decDegrees
	
	return decDegrees

#
# Date / Time Conversions:

def ymd2jd(year, month, day):
	if month == 1 or month == 2:
		yprime = year - 1
		mprime = month + 12
	else:
		yprime = year
		mprime = month
	
	if year > 1582 or (year == 1582 and (month >= 10 and day >= 15)):
		A = int(yprime / 100)
		B = 2 - A + int(A/4.0)
	else:
		B = 0	
		
	if yprime < 0:
		C = int((365.25 * yprime) - 0.75)
	else:
		C = int(365.25 * yprime)
	
	D = int(30.6001 * (mprime + 1))
	
	return B + C + D + day + 1720994.5

def utcDatetime2gmst(datetimeObj):
	jd = ymd2jd(datetimeObj.year, datetimeObj.month, datetimeObj.day)
	
	S = jd - 2451545.0
	T = S / 36525.0
	T0 = 6.697374558 + (2400.051336 * T) + (0.000025862 * T**2)
	T0 = T0 % 24
	
	UT = datetime2decHours(datetimeObj.time()) * 1.002737909
	T0 += UT
	
	GST = T0 % 24

	h,m,s = dec2sex(GST)
	return datetime.datetime(year=datetimeObj.year, month=datetimeObj.month, day=datetimeObj.day, hour=h, minute=m, second=int(s), microsecond=int((s-int(s))*10**6))
	
def gmst2utcDatetime(datetimeObj):
	jd = ymd2jd(datetimeObj.year, datetimeObj.month, datetimeObj.day)
	
	S = jd - 2451545.0
	T = S / 36525.0
	T0 = 6.697374558 + (2400.051336*T) + (0.000025862*T**2)
	T0 = T0 % 24
	
	GST = (datetime2decHours(datetimeObj.time()) - T0) % 24
	UT = GST * 0.9972695663
	
	h,m,s = dec2sex(UT)
	return datetime.datetime(year=datetimeObj.year, month=datetimeObj.month, day=datetimeObj.day,
								hour=h, minute=m, second=int(s), microsecond=int((s-int(s))*10**6))

def mjd2jd(mjd):
	return mjd + 2400000.5

def jd2mjd(jd):
	return float(jd - 2400000.5)

def jd2datetime(jd):
	jd = jd + 0.5
	(jd_frac, jd_int) = math.modf(jd)
	
	if jd_int > 2299160:
		A = int((jd_int - 1867216.25)/36524.25)
		B = jd_int + 1 + A - int(A/4)
	else:
		B = jd_int
		
	C = B + 1524
	D = int((C - 122.1) / 365.25)
	E = int(365.25 * D)
	G = int((C - E) / 30.6001)

	d = C - E + jd_frac - int(30.6001 * G) 
	
	if G < 13.5:
		month = G - 1
	else:
		month = G - 13
	month = int(month)
		
	if month > 2.5:
		y = D - 4716
	else:
		y = D - 4715 # year
	y = int(y)
	
	(d_frac, d) = math.modf(d)
	d = int(d)
	h = d_frac*24 # fractional part of day * 24 hours

	(h, min, s) = dec2sex(h)
	(s_frac, s) = math.modf(s)
	ms = s_frac * 60 * 1000

	s = int(s)
	ms = int(s)

	return datetime.datetime(y, month, d, h, min, s, ms)
	
def mjd2datetime(mjd):
	jd = mjd2jd(mjd)
	return jd2datetime(jd)

def datetime2jd(datetimeObj):
	A = ymd2jd(datetimeObj.year, datetimeObj.month, datetimeObj.day)
	B = datetime2decHours(datetimeObj.time()) / 24.0
	return A + B

def datetime2mjd(datetimeObj):
	jd = datetime2jd(datetimeObj)
	return jd2mjd(jd)

def gmst2lst(longitude, hour, minute=None, second=None, lonDirection='W', lonUnits='DEGREES'):
	if minute != None and second != None:
		hours = sex2dec(hour, minute, second)
	elif minute == None and second == None:
		hours = hour
	else:
		raise AssertionError('minute and second must either be both set, or both unset.')
	
	if lonUnits.upper() == 'DEGREES':
		lonTime = longitude / 15.0
	elif lonUnits.upper() == 'RADIANS':
		lonTime = longitude * 180.0 / math.pi / 15.0
	
	if lonDirection.upper() == 'W':
		lst = hours - lonTime
	elif lonDirection.upper() == 'E':
		lst = hours + lonTime
	else:
		raise AssertionError('lonDirection must be W or E')
	
	lst = lst % 24.0
	
	return dec2sex(lst)
	
def lst2gmst(longitude, hour, minute=None, second=None, lonDirection='W', lonUnits='DEGREES'):
	if minute != None and second != None:
		hours = sex2dec(hour, minute, second)
	elif minute == None and second == None:
		hours = hour
	else:
		raise AssertionError('minute and second must either be both set, or both unset.')
	
	if lonUnits.upper() == 'DEGREES':
		lonTime = longitude / 15.0
	elif lonUnits.upper() == 'RADIANS':
		lonTime = longitude * 180.0 / math.pi / 15.0
	
	if lonDirection.upper() == 'W':
		gmst = hours + lonTime
	elif longitudeDirection.upper() == 'E':
		gmst = hours - lonTime
	else:
		raise AssertionError('longitudeDirection must be W or E')
	
	gmst = gmst % 24.0
	
	return dec2sex(gmst)

def mjdLon2lst(mjd, longitude, lonDirection='W', lonUnits='DEGREES'):
	dt = mjd2datetime(mjd)
	gmst = utcDatetime2gmst(dt)
	return gmst2lst(longitude, hour=gmst.hour, minute=gmst.minute, second=(gmst.second+gmst.microsecond/10**6),
					lonDirection=lonDirection, lonUnits=lonUnits)
