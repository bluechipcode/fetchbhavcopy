#!/usr/bin/env python3.6

#Script created by Venkatesan - released under GNU/GPL V3

bhavcopyurl='http://www.nseindia.com/content/historical/EQUITIES/2007/DEC/cm11DEC2007bhav.csv';
#http://www.nseindia.com/content/historical/DERIVATIVES/2008/JAN/fo22JAN2008bhav.csv

#NSE
#https://www.nseindia.com/content/historical/EQUITIES/2017/APR/cm19APR2017bhav.csv.zip
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

from datetime import date
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *

import os
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
                        'fname':"cm%02d%s%dbhav.csv.zip",
                        'uzfname':"cm%02d%s%dbhav.csv",
                        'start_date':datetime(1994,11,3),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month-1]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month-1],d.year),
                        'fexists': lambda f,uzf: os.path.exists(f) or os.path.exists(uzf) 
                },
                "eod_fo" : {
                        'url':'https://www.nseindia.com/content/historical/DERIVATIVES/%d/%s',
                        'fname':"fo%02d%s%dbhav.csv.zip",
                        'uzfname':"fo%02d%s%dbhav.csv",
                        'start_date':datetime(2000,6,12),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month-1]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month-1],d.year),
                        'fexists': lambda f,uzf: os.path.exists(f) or os.path.exists(uzf) 
                },
                "eod_mto" : {
                        'url':'https://www.nseindia.com/archives/equities/mto',
                        'fname':"MTO_%02d%02d%02d.DAT",
                        'uzfname':None,
                        'start_date':datetime(2002,1,1),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                        'fexists': lambda f,uzf: os.path.exists(f) 

                },
                "eod_shortsell" : {
                        'url':'https://www.nseindia.com/archives/equities/shortSelling',
                        'fname':"shortselling_%02d%02d%d.csv",
                        'uzfname':None,
                        'start_date':datetime(2012,7,17),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                        'fexists': lambda f,uzf: os.path.exists(f) 
                },
                "eod_vol" : {
                        'url':'https://www.nseindia.com/archives/nsccl/volt',
                        'fname':"CMVOLT_%02d%02d%d.CSV",
                        'uzfname': None,
                        'start_date':datetime(2011,4,17),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                        'fexists': lambda f,uzf: os.path.exists(f) 
                },

        },
        'bse': {
                "eod_stk" : {
                        'url':'http://www.bseindia.com/download/BhavCopy/Equity',
                        'fname':"eq%02d%02d%02d_csv.zip",
                        'uzfname':"eq%02d%02d%02d.csv",
                        'start_date':datetime(2007,7,7),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%2000),
                        'fexists': lambda f,uzf: os.path.exists(f) or os.path.exists(uzf) 
                },
                "eod_fo" : {
                        'url':'http://www.bseindia.com/download/Bhavcopy/Derivative',
                        'fname':"bhavcopy%02d-%02d-%02d.zip",
                        'uzfname':"bhavcopy%02d-%02d-%02d.xls",
                        'start_date':datetime(2008,1,11),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%2000),
                        'fexists': lambda f,uzf: os.path.exists(f) or os.path.exists(uzf) 
                },
        },

}

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
def fetch_files(exch, type):
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
        start_date=sdmap[exch][type]['start_date']
        filet=sdmap[exch][type]['fname']
        uzfilet=sdmap[exch][type]['uzfname']
        urlt=sdmap[exch][type]['url']
        get_url=sdmap[exch][type]['get_url']
        get_file=sdmap[exch][type]['get_file']
        fexists=sdmap[exch][type]['fexists']
        
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
                        uzfname = None if uzfilet == None else get_file(uzfilet,i)

                        if  fexists(fname, uzfname):
                                log.debug("Skipping %s (already exists)" % fname)
                        else:
                                url = "%s/%s" % (get_url(urlt,i), fname)
                                if  url  in not_found:
                                        log.debug( 'In not found list, skipping  [%s]' % fname)
                                else:
                                        store_url_to_file(hdr, url, fname, retry_file, not_found,i.date()==today)

        summary.append(("%s [%s]" % (exch, type), str(len(not_found))));
        if len(not_found) > not_found_from_file:
                with open("not_found.txt" ,"w") as f:
                        for l in sorted(not_found):
                                f.write(l+"\n")

        os.chdir(curdir)         
        log.debug("Changed back to dir [%s]" % os.getcwd())                

if __name__ == "__main__":
        parser = argparse.ArgumentParser(description='Fetch bhavcopy archives from NSE/BSE',
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("-d","--dump-dir",help="Directory to dump the files to", default="dumps/bhavcopy")
        args = parser.parse_args()

        log.debug(str(args))
        log.info("Dumping to dir [%s]" % args.dump_dir)
        os.makedirs(args.dump_dir, exist_ok=True)
        os.chdir(args.dump_dir)

        fetch_files("nse", "eod_stk")
        fetch_files("nse", "eod_fo")
        fetch_files("nse", "eod_mto")
        fetch_files("nse", "eod_shortsell")
        fetch_files("nse", "eod_vol")
        fetch_files("bse", "eod_stk")
        fetch_files("bse", "eod_fo")

        log.info("All fetch complete, summary:")
        log.info(print_table(summary))

