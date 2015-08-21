# -*- coding:utf-8 -*-
from __future__ import division
from __future__ import print_function
import math

__version__ = '1.0'

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


# Some tests.
def _test_gcal2jd_with_sla_cldj():
	'''Compare gcal2jd with slalib.sla_cldj.'''
	import random
	try:
		from pyslalib import slalib
	except ImportError:
		print('SLALIB (PySLALIB not available).')
		return 1
	n = 1000
	mday = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

	# sla_cldj needs year > -4699 i.e., 4700 BC.
	year = [random.randint(-4699, 2200) for i in range(n)]
	month = [random.randint(1, 12) for i in range(n)]
	day = [random.randint(1, 31) for i in range(n)]
	for i in range(n):
		x = 0
		if is_leap(year[i]) and month[i] == 2:
			x = 1
		if day[i] > mday[month[i]] + x:
			day[i] = mday[month[i]]

	jd_jdc = [gcal2jd(y, m, d)[1]
			  for y, m, d in zip(year, month, day)]
	jd_sla = [slalib.sla_cldj(y, m, d)[0]
			  for y, m, d in zip(year, month, day)]
	diff = [abs(i - j) for i, j in zip(jd_sla, jd_jdc)]
	assert max(diff) <= 1e-8
	assert min(diff) <= 1e-8


def _test_jd2gcal():
	'''Check jd2gcal as reverse of gcal2jd.'''
	import random
	n = 1000
	mday = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

	year = [random.randint(-4699, 2200) for i in range(n)]
	month = [random.randint(1, 12) for i in range(n)]
	day = [random.randint(1, 31) for i in range(n)]
	for i in range(n):
		x = 0
		if is_leap(year[i]) and month[i] == 2:
			x = 1
		if day[i] > mday[month[i]] + x:
			day[i] = mday[month[i]]

	jd = [gcal2jd(y, m, d)[1]
		  for y, m, d in zip(year, month, day)]

	x = [jd2gcal(MJD_0, i) for i in jd]

	for i in range(n):
		assert x[i][0] == year[i]
		assert x[i][1] == month[i]
		assert x[i][2] == day[i]
		assert x[i][3] <= 1e-15


def _test_jd2jcal():
	'''Check jd2jcal as reverse of jcal2jd.'''
	import random
	n = 1000
	year = [random.randint(-4699, 2200) for i in range(n)]
	month = [random.randint(1, 12) for i in range(n)]
	day = [random.randint(1, 28) for i in range(n)]

	jd = [jcal2jd(y, m, d)[1]
		  for y, m, d in zip(year, month, day)]

	x = [jd2gcal(MJD_0, i) for i in jd]

	for i in range(n):
		assert x[i][0] == year[i]
		assert x[i][1] == month[i]
		assert x[i][2] == day[i]
		assert x[i][3] <= 1e-15
