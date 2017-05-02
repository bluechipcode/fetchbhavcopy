#bhavcopyurl='http://www.nseindia.com/content/historical/EQUITIES/2007/DEC/cm11DEC2007bhav.csv';
#http://www.nseindia.com/content/historical/DERIVATIVES/2008/JAN/fo22JAN2008bhav.csv
from urllib2 import URLError
from datetime import date
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *

import commands
import os
import urllib2
import psycopg2
import socket

def start():
	conn=psycopg2.connect("dbname=svtesan")
	curs=conn.cursor()

	#set the global socket timeout
	socket.setdefaulttimeout(100)
	
	update(conn, curs, "stock")
	update(conn, curs, "fo")

def update(conn, curs, instrument):
	print "Updating historical_",instrument,"data..."
	#see what the last date in database is
	dates = get_daterange_from_db(curs, instrument)
	update_eod_data(dates, conn, curs, instrument)

	#update_missing_eod(conn, curs, instrument)

def update_missing_eod(conn, curs, instrument):
	monthstr=['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
	dates=[]

	#yyyy,mm,dd
	if instrument == "stock":
		starting_date = datetime(1994,11,3)
	else: #fo
		#starting_date = datetime(2000,6,12)
		starting_date = datetime(2006,1,1)
		
	for i in list(rrule(DAILY, dtstart=starting_date, until=datetime.now(), byweekday=(MO,TU,WE,TH,FR))):
		print 'Checking ', i,
		curs.execute("select timestamp from historical_%s_data where timestamp='%d %s %d'" % (instrument, i.day, monthstr[i.month-1], i.year))
		r=curs.fetchall()
		if r == []:
			print ' not found, adding to fetch list'
			dates.append(i)
		else:
			print ' found'

	update_common(dates, conn, curs, instrument)

def get_daterange_from_db(curs, instrument):
	dates=[]
	query = "select max(timestamp) from historical_%s_data"%(instrument)
	curs.execute(query)
	rows=curs.fetchall()

	#fetch nse data from the day after last day in db to today
	dates.append(rows[0][0] + relativedelta(days=1))
	dates.append(date.today())
	return dates

def update_eod_data(dates, conn, curs, instrument):
	update_common(list(rrule(DAILY, dtstart=dates[0], until=dates[1], byweekday=(MO,TU,WE,TH,FR))), conn, curs, instrument)

def update_common(datelist, conn, curs, instrument):
	monthstr=['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];

	if instrument == "stock":
		bhavcopyurl='http://www.nseindia.com/content/historical/EQUITIES/%d/%s/cm%02d%s%dbhav.csv';
	else:
		bhavcopyurl='http://www.nseindia.com/content/historical/DERIVATIVES/%d/%s/fo%02d%s%dbhav.csv';

	pgtable='historical_%s_data'%(instrument)
	retry_file=open('/tmp/retry_file','w')

	j=0
	for i in datelist:
		month = monthstr[i.month-1]
		url=bhavcopyurl%(i.year,month,i.day,month,i.year);
		table=pgtable

		try:
			print 'Fetching: ',  url 
			file=urllib2.urlopen(url)
			print '=>OK'
			print 'Reading from url ... '
			file.readline() #discard the header
			print '=>OK'

			try:
				print 'Initiating copy ...'
				curs.copy_from(file, table, ',')
				print '=>OK'

			except StandardError,e:
				conn.commit()
				print 'FAIL ',e
				print 'RETRY: ', url
				retry_file.writelines(url+'\n')

		except KeyboardInterrupt:
				print "Aborting at user's request. Please be aware that a commit will be done."
				break

		except URLError,e:
			print '=>not found'

		except socket.timeout:
			print '=>timed out. Proceeding to next item.'
	

		if j == 22:
			print 'Commit after 200 entries ...'
			conn.commit()
			print '=>OK'

			j=0

		j = j + 1

	retry_file.close()
	print "Performing a final commit ..."
	conn.commit()
	print "=>OK"

start()
