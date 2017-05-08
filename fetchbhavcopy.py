#!/usr/bin/env python3.6

#Script created by Venkatesan - released under GNU/GPL V3

#NSE
#https://www.nseindia.com/content/historical/EQUITIES/2017/APR/cm19APR2017bhav.csv.zip
#http://www.nseindia.com/content/historical/DERIVATIVES/2008/JAN/fo22JAN2008bhav.csv.zip
#https://www.nseindia.com/archives/nsccl/var/C_VAR1_19042017_1.DAT
#..upto _6.DAT
#https://www.nseindia.com/archives/nsccl/volt/CMVOLT_28032011.CSV
#https://www.nseindia.com/archives/equities/mkt/MA200417.csv
#https://www.nseindia.com/archives/equities/mto/MTO_01012002.DAT
#https://www.nseindia.com/archives/equities/margin/Margintrdg_200417.zip
#https://www.nseindia.com/archives/equities/cat/cat_turnover_200417.xls
#https://www.nseindia.com/archives/equities/shortSelling/shortselling_17072012.csv
#https://www.nseindia.com/archives/combine_report/combined_report20042017.zip


#BSE, earliest
#http://www.bseindia.com/download/BhavCopy/Equity/eq230707_csv.zip
#http://www.bseindia.com/download/Bhavcopy/Derivative/bhavcopy11-01-08.zip

from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *

import os
import glob
import sys
import argparse
import socket
import urllib
from urllib.request import Request, urlopen
import logging

log = logging.getLogger(sys.argv[0])
log.setLevel(logging.DEBUG)

fh = logging.FileHandler('%s.log' % sys.argv[0])
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh.setFormatter(formatter)
ch.setFormatter(formatter)

log.addHandler(fh)
log.addHandler(ch)

monthstr=['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

sdmap = {
        'nse': {
                "eod_stk" : {
                        'url':'https://www.nseindia.com/content/historical/EQUITIES/%d/%s',
                        'fname':"cm%02d%s%dbhav",
                        'ext':".csv.zip",
                        'start_date':datetime(1994,11,3).date(),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month-1]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month-1],d.year),
                },
                "eod_fo" : {
                        'url':'https://www.nseindia.com/content/historical/DERIVATIVES/%d/%s',
                        'fname':"fo%02d%s%dbhav",
                        'ext':".csv.zip",
                        'start_date':datetime(2000,6,12).date(),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month-1]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month-1],d.year),
                },
                "eod_mto" : {
                        'url':'https://www.nseindia.com/archives/equities/mto',
                        'fname':"MTO_%02d%02d%02d",
                        'ext':'.DAT',
                        'start_date':datetime(2002,1,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),

                },
                "eod_shortsell" : {
                        'url':'https://www.nseindia.com/archives/equities/shortSelling',
                        'fname':"shortselling_%02d%02d%d",
                        'ext':'.csv',                        
                        'start_date':datetime(2012,7,17).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "eod_vol" : {
                        'url':'https://www.nseindia.com/archives/nsccl/volt',
                        'fname':"CMVOLT_%02d%02d%d",
                        'ext':'.CSV',
                        'start_date':datetime(2011,4,17).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },

        },
        'bse': {
                "eod_stk" : {
                        'url':'http://www.bseindia.com/download/BhavCopy/Equity',
                        'fname':"eq%02d%02d%02d",
                        'ext':'_csv.zip',
                        'start_date':datetime(2007,7,7).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%2000),
                },
                "eod_fo" : {
                        'url':'http://www.bseindia.com/download/Bhavcopy/Derivative',
                        'fname':"bhavcopy%02d-%02d-%02d",
                        'ext':'.zip',
                        'start_date':datetime(2008,1,11).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%2000),
                },
        },
}

def fexists(pat):
        return bool(glob.glob(pat+'*.*'))

#mon, day
holidays={(1,1), #new year
          (1, 26), #republic day
          (1,30), #gandhi memory day
          (4,14), #regional new year
          (5,1), #may day
          (8,15), #independence day
          (10,2), #gandhi jayanthi
          (12,25), #christmas
        }

hdrmap= { 'nse' : {'User-Agent': 'mybot', 'Accept': '*/*',
                "Referer": "https://www.nseindia.com/products/content/equities/equities/archieve_eq.htm"},
        'bse' : {},
}

def store_url_to_file(hdr, url, fname, retry_file, not_found,istoday=None):
        iserr=True
        try:
                log.debug( 'Fetching  %s' % url)
                req = Request(url, headers=hdr)
                data=urlopen(req).read()
                                
                with open(fname,"wb") as f:
                        f.write(data)
                        log.info('wrote [%s]' % fname)
                        iserr=False
        except urllib.error.HTTPError as e:
                log.error("Error fetching [%s]: %s" % (fname, e))
                if e.code == 404:
                        iserr=False #file was not found, not an error
                        if istoday == None or not istoday:
                                not_found.add(url)
        except:
                log.error("Error fetching [%ss]: %s" % (fname, str(sys.exc_info()[0])))

        if iserr:
                retry_file.write(url + "\n")

def print_table(table):
    col_width = [max(len(x) for x in col) for col in zip(*table)]
    rval = "\n"
    for line in table:
        rval += "| " + " | ".join("{:{}}".format(x, col_width[i])
                                for i, x in enumerate(line)) + " |\n"
    return rval
        

summary=[("Exchange [Type]", "#NotFound")]
def fetch_files(exch, type, args):
        #set the global socket timeout
        socket.setdefaulttimeout(100)
        curdir=os.getcwd()
        dirname="%s/%s" % (exch, type)
        try:
                os.makedirs(dirname)
        except:
                pass
        os.chdir(dirname)

        log.info("Processing %s %s" % (exch,type))
        log.debug("Changed to dir [%s]" % os.getcwd())
        hdr=hdrmap[exch]
        curmap=sdmap[exch][type]
        start_date=curmap['start_date'] if args.fetch_all else max(curmap['start_date'], args.start_date)
        filet=curmap['fname']
        ext=curmap['ext']
        urlt=curmap['url']
        get_url=curmap['get_url']
        get_file=curmap['get_file']
        
        not_found=set()
        if os.path.exists("not_found.txt"):
                with open("not_found.txt" ,"r") as f:
                        for l in f:
                                not_found.add(l.rstrip())

        not_found_from_file=len(not_found)
        
        if os.path.exists("retry.txt"):
                os.rename("retry.txt", "retry.proc.txt")
                with open("retry.txt", "w") as retry_file, open("not_found.txt","a+") as not_found_file, open("retry.proc.txt","r") as f :
                        for url in f:
                                url = url.rstrip()
                                fname=url.split('/')[-1]
                                store_url_to_file(hdr, url, fname, retry_file, not_found)

                os.remove("retry.proc.txt")
                
        if len(not_found) > not_found_from_file:
                with open("not_found.txt" ,"w") as f:
                        for l in not_found:
                                f.write(l+"\n")

        not_found_from_file = len(not_found)
        today=datetime.now().date()
        
        with open("retry.txt","w") as retry_file:
                for i in list(rrule(DAILY, dtstart=start_date, until=today, byweekday=(MO,TU,WE,TH,FR))):
                        if (i.month, i.day) in holidays:
                                log.debug('Skipping holiday: %s' % str(i))
                                continue
                        
                        log.debug( 'Fetching for %s' % str(i))
                        fname = get_file(filet,i)

                        if  fexists(fname):
                                log.debug("Skipping %s (already exists)" % fname)
                        else:
                                url = "%s/%s%s" % (get_url(urlt,i), fname, ext)
                                if  url  in not_found:
                                        log.debug( 'In not found list, skipping  [%s]' % fname)
                                else:
                                        store_url_to_file(hdr, url, fname+ext, retry_file, not_found,i.date()==today)

        summary.append(("%s [%s]" % (exch, type), str(len(not_found))));
        if len(not_found) > not_found_from_file:
                with open("not_found.txt" ,"w") as f:
                        for l in sorted(not_found):
                                f.write(l+"\n")

        os.chdir(curdir)         
        log.debug("Changed back to dir [%s]" % os.getcwd())                

def valid_start_date(s):
    try:
        s=s.upper()
        p = s.split()
        lp = len(p)

        yr=datetime.now().year
        mon=1
        dd=1
        
        if lp == 1:
                #yr/mon given
                if p[0] in monthstr:
                        mon = 1 + monthstr.index(p[0])
                else:
                        yr=int(p[0])
                        
                return datetime(yr,mon,dd).date()
        elif lp==2:
                mon = 1 + monthstr.index(p[0])
                yr=int(p[1])
                return datetime(yr,mon,dd).date()
        elif lp==3:
                #all 3 given
                return datetime.strptime(s, '%d %b %Y').date()
                
        else:
                raise ValueError()
    except:
        msg = "Invalid start date: [{0}].".format(s)
        raise argparse.ArgumentTypeError(msg)

class MyFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass

if __name__ == "__main__":
        default_start=datetime.now().date() - timedelta(weeks=2)
        parser = argparse.ArgumentParser(description="Fetch bhavcopy archives from NSE/BSE.\n"
                                         + "    By default, only files from last 2 weeks are fetched - can be overridden by  switches below.\n"
                                         + "    Typical usage is to download all data for the first time and then use default (last 2 weeks only) \n"
                                         + "    to speed up the fetch\n"
                                         + "\nSample usage:\n"
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s "20 apr 2017"\n'
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s "20 APR" #downloads from given date of current year\n'
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s 2015 #from jan 1 2015\n'
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s feb #from feb 1 of current year, case of month string doesnt matter\n'
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -a #all available data - 20+ years for equities\n'
                                         + '  fetchbhavcopy.py -d ..\data\dumps\bhavcopy  #for last 2 weeks - default\n'
                                         + '  fetchbhavcopy.py   #for last 2 weeks and use default dump directory\n'
                                         ,formatter_class=MyFormatter)
        
        parser.add_argument("-d","--dump-dir",help="Directory to dump the files to", default="dumps/bhavcopy")
        parser.add_argument("-a","--fetch-all",action='store_true',help="Download from start of history")
        parser.add_argument("-s","--start-date",help="Specify a starting date \n"
                            + "Any valid year can be specified, but actual year used depends on data available \n"
                            + "Eg. NSE equities bhavcopy is available only from 1994 so year=max(year specified, 1994)\n"
                            + "Accepted formats:\n"
                            + "* yyyy (From 1 JAN of given year, eg. 1999)\n"
                            + "* mon yyyy (From day 1, eg. JAN 1999)\n"
                            + "* dd mon yyyy (eg. 12 JAN 1999)\n"
                            + "* mon (From current year, day 1 of given month, eg. JAN)\n",
                            type=valid_start_date,
                            default=default_start)
        args = parser.parse_args()

        log.debug("args=%s" % str(args))
        if  args.fetch_all:
                log.info(default_start.strftime("Fetching all available data - might take quite some time, look at --help for other options"))
        else:
                if args.start_date == default_start:
                        log.info(default_start.strftime("Fetching only from  [%d %b %Y], look at --help for other options"))
                else:
                        log.info(args.start_date.strftime("Fetching from  [%d %b %Y], look at --help for other options"))

        log.info("Dumping to dir [%s]" % args.dump_dir)
        os.makedirs(args.dump_dir, exist_ok=True)
        os.chdir(args.dump_dir)

        fetch_files("nse", "eod_stk", args)
        fetch_files("nse", "eod_fo", args)
        fetch_files("nse", "eod_mto", args)
        fetch_files("nse", "eod_shortsell", args)
        fetch_files("nse", "eod_vol", args)
        fetch_files("bse", "eod_stk", args)
        fetch_files("bse", "eod_fo", args)

        log.info("All fetch complete, summary:")
        log.info(print_table(summary))

