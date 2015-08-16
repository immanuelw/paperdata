from flask import g
from datetime import datetime
from requests_futures.sessions import FuturesSession

def send_query(db, query):
    cur = db.cursor()
    cur.execute(query)
    return cur

def get_gps_utc_constants():
    leap_seconds_result = send_query(g.eor_db,
        "SELECT leap_seconds FROM leap_seconds ORDER BY leap_seconds DESC LIMIT 1").fetchone()

    leap_seconds = leap_seconds_result[0]

    GPS_LEAP_SECONDS_OFFSET = leap_seconds - 19

    jan_1_1970 = datetime(1970, 1, 1)

    jan_6_1980 = datetime(1980, 1, 6)

    GPS_UTC_DELTA = (jan_6_1980 - jan_1_1970).total_seconds()

    return (GPS_LEAP_SECONDS_OFFSET, GPS_UTC_DELTA)

def get_gps_from_datetime(start_datetime, end_datetime):
    session = FuturesSession()

    baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?utciso='

    requestURLStart = baseUTCToGPSURL + start_datetime.strftime('%Y-%m-%dT%H:%M:%S')

    requestURLEnd = baseUTCToGPSURL + end_datetime.strftime('%Y-%m-%dT%H:%M:%S')

    #Start the first Web service request in the background.
    future_start = session.get(requestURLStart)

    #The second request is started immediately.
    future_end = session.get(requestURLEnd)

    #Wait for the first request to complete, if it hasn't already.
    start_gps = int(future_start.result().content)

    #Wait for the second request to complete, if it hasn't already.
    end_gps = int(future_end.result().content)

    return (start_gps, end_gps)

def get_datetime_from_gps(start_gps, end_gps):
    session = FuturesSession()

    baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?gpssec='

    requestURLStart = baseUTCToGPSURL + str(start_gps)

    requestURLEnd = baseUTCToGPSURL + str(end_gps)

    #Start the first Web service request in the background.
    future_start = session.get(requestURLStart)

    #The second request is started immediately.
    future_end = session.get(requestURLEnd)

    #Wait for the first request to complete, if it hasn't already.
    start_datetime = datetime.strptime(future_start.result().content.decode('utf-8'), '"%Y-%m-%dT%H:%M:%S"')

    #Wait for the second request to complete, if it hasn't already.
    end_datetime = datetime.strptime(future_end.result().content.decode('utf-8'), '"%Y-%m-%dT%H:%M:%S"')

    return (start_datetime, end_datetime)

def get_lowhigh_and_eor_clauses(low_or_high, eor):
    low_high_clause = "" if low_or_high == 'any' else "AND obsname LIKE '" + low_or_high + "%'"

    eor_clause = ''

    if eor != 'any':
        if eor == 'EOR0':
            eor_clause = "AND ra_phase_center = 0"
        else:
            eor_clause = "AND ra_phase_center = 60"

    return (low_high_clause, eor_clause)
