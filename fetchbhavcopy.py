#!/usr/bin/env python3.6

#Script created by Venkatesan - released under GNU/GPL V3

#https://www.nseindia.com/archives/nsccl/var/C_VAR1_19042017_1.DAT
#..upto _6.DAT

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
from threading import Thread

log = logging.getLogger(sys.argv[0])
monthstr=['INVALID', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

sdmap = {
        'nse': {
                "eod_stk" : {
                        'eg': 'https://www.nseindia.com/content/historical/EQUITIES/2017/APR/cm19APR2017bhav.csv.zip',
                        'url':'https://www.nseindia.com/content/historical/EQUITIES/%d/%s',
                        'fname':"cm%02d%s%dbhav",
                        'ext':".csv.zip",
                        'start_date':datetime(1994,11,3).date(),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "eod_fo" : {
                        'eg':'https://www.nseindia.com/content/historical/DERIVATIVES/2008/JAN/fo22JAN2008bhav.csv.zip',
                        'url':'https://www.nseindia.com/content/historical/DERIVATIVES/%d/%s',
                        'fname':"fo%02d%s%dbhav",
                        'ext':".csv.zip",
                        'start_date':datetime(2000,6,12).date(),
                        'get_url':lambda urlt,d:urlt%(d.year,monthstr[d.month]),
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "eod_mto" : {
                        'eg':'https://www.nseindia.com/archives/equities/mto/MTO_01012002.DAT',
                        'url':'https://www.nseindia.com/archives/equities/mto',
                        'fname':"MTO_%02d%02d%02d",
                        'ext':'.DAT',
                        'start_date':datetime(2002,1,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),

                },
                "eod_shortsell" : {
                        'eg':'https://www.nseindia.com/archives/equities/shortSelling/shortselling_17072012.csv',
                        'url':'https://www.nseindia.com/archives/equities/shortSelling',
                        'fname':"shortselling_%02d%02d%d",
                        'ext':'.csv',                        
                        'start_date':datetime(2012,7,17).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "eod_vol" : {
                        'eg':'https://www.nseindia.com/archives/nsccl/volt/CMVOLT_28032011.CSV',
                        'url':'https://www.nseindia.com/archives/nsccl/volt',
                        'fname':"CMVOLT_%02d%02d%d",
                        'ext':'.CSV',
                        'start_date':datetime(2011,4,17).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "ind_close": {
                        'eg':'https://nseindia.com/content/indices/ind_close_all_09052017.csv',
                        'url': 'https://nseindia.com/content/indices',
                        'fname':'ind_close_all_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2012,2,21).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "margintrd": {
                        'eg':'https://www.nseindia.com/archives/equities/margin/Margintrdg_130404.zip',
                        'url': 'https://www.nseindia.com/archives/equities/margin',
                        'fname':'Margintrdg_%02d%02d%02d',
                        'ext':'.zip',
                        'start_date':datetime(2004,4,13).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "cat_to": {
                        'eg':'https://www.nseindia.com/archives/equities/cat/cat_turnover_110405.xls',
                        'url': 'https://www.nseindia.com/archives/equities/cat',
                        'fname':'cat_turnover_%02d%02d%02d',
                        'ext':'.xls',
                        'start_date':datetime(2005,4,11).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "combrep": {
                        'eg':'https://www.nseindia.com/archives/combine_report/combined_report06082012.zip',
                        'url': 'https://www.nseindia.com/archives/combine_report',
                        'fname':'combined_report%02d%02d%d',
                        'ext':'.zip',
                        'start_date':datetime(2012,8,6).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "seccateg": {
                        'eg':'https://www.nseindia.com/archives/nsccl/mult/C_CATG_APR2003.T01',
                        'url': 'https://www.nseindia.com/archives/nsccl/mult',
                        'fname':'C_CATG_%s%d',
                        'ext':'.T01',
                        'start_date':datetime(2012,8,6).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(monthstr[d.month],d.year),
                        'freq':'month',
                },
                #https://www.nseindia.com/archives/equities/csqr/CSQR_N2017001_04012017.csv
                #more complex pattern, first date is this year but till 4th, it has 2016
                #name seems to be: CSQR_Nyyyy<business day-3>_ddmmyyyy.csv
                #so, first 3 days of an year are the previous ones. guess this mirrors T+3 settling
                

                "mcwb": {
                        'eg':'https://www.nseindia.com/content/indices/mcwb_jan08.zip',
                        'url': 'https://www.nseindia.com/content/indices',
                        'fname':'mcwb_%s%d',
                        'ext':'.zip',
                        'start_date':datetime(2010,1,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(monthstr[d.month].lower(),d.year-2000),
                        'freq':'month',
                },
                "imp_cost": {
                        'eg':'https://nseindia.com/content/indices/ind_ic_2001.zip',
                        'url': 'https://nseindia.com/content/indices',
                        'fname':'ind_ic_%d',
                        'ext':'.zip',
                        'start_date':datetime(2001,1,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.year),
                        'freq':'year',
                },
                "clifund1": {
                        'eg':'https://www.nseindia.com/archives/equities/client/cli_fund_oct04.xls',
                        'url': 'https://www.nseindia.com/archives/equities/client',
                        'fname':'cli_fund_%s%d',
                        'ext':'.xls',
                        'start_date':datetime(2004,10,1).date(),
                        'end_date':datetime(2006,6,30).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(monthstr[d.month].lower(),d.year-2000),
                },
                "clifund1": {
                        'eg':'https://www.nseindia.com/archives/equities/client/cli_fund_jul06.csv',
                        'url': 'https://www.nseindia.com/archives/equities/client',
                        'fname':'cli_fund_%s%d',
                        'ext':'.csv',
                        'start_date':datetime(2006,7,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(monthstr[d.month].lower(),d.year-2000),
                },
                "ind_mcw": {
                        'eg':'https://nseindia.com/content/indices/indices_dataApr2013.zip',
                        'url': 'https://nseindia.com/content/indices',
                        'fname':'indices_data%s%d',
                        'ext':'.zip',
                        'start_date':datetime(2014,4,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(monthstr[d.month].title(),d.year),
                        'freq':'month',
                },
                "fo_secban": {
                        'eg':'https://nseindia.com/archives/fo/sec_ban/fo_secban_01012008.csv',
                        'url': 'https://nseindia.com/archives/fo/sec_ban',
                        'fname':'fo_secban_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2008,1,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "mar1": {
                        'eg':'https://www.nseindia.com/archives/equities/mkt/mkt03012000.doc',
                        'url': 'https://www.nseindia.com/archives/equities/mkt',
                        'fname':'mkt%02d%02d%d',
                        'ext':'.doc',
                        'start_date':datetime(2000,1,3).date(),
                        'end_date':datetime(2012,2,17).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "mar2": {
                        'eg':'https://www.nseindia.com/archives/equities/mkt/MA200417.csv',
                        'url': 'https://www.nseindia.com/archives/equities/mkt',
                        'fname':'MA%02d%02d%02d',
                        'ext':'.csv',
                        'start_date':datetime(2012,2,21).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%100),
                },
                "fo_rep1": {
                        'eg':'https://www.nseindia.com/archives/fo/mkt/fo050701.doc',
                        'url': 'https://www.nseindia.com/archives/fo/mkt',
                        'fname':'fo%02d%02d%02d',
                        'ext':'.doc',
                        'start_date':datetime(2001,7,5).date(),
                        'end_date':datetime(2011,2,22).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%100),
                },
                "fo_rep2": {
                        'eg':'https://www.nseindia.com/archives/fo/mkt/fo23022011.ZIP',
                        'url': 'https://www.nseindia.com/archives/fo/mkt',
                        'fname':'fo%02d%02d%d',
                        'ext':'.zip',
                        'start_date':datetime(2011,2,23).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "fosett_prce": {
                        'eg':'https://nseindia.com/archives/nsccl/sett/FOSett_prce_19102001.csv',
                        'url': 'https://nseindia.com/archives/nsccl/sett',
                        'fname':'FOSett_prce_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2001,10,19).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "cli_poslimit": {
                        'eg':'https://nseindia.com/archives/nsccl/cli/oi_cli_limit_02-JAN-2002.lst',
                        'url': 'https://nseindia.com/archives/nsccl/cli',
                        'fname':'oi_cli_limit_%02d-%s-%d',
                        'ext':'.lst',
                        'start_date':datetime(2002,1,2).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "nseoi": {
                        'eg':'https://www.nseindia.com/archives/nsccl/mwpl/nseoi_01022010.zip',
                        'url': 'https://www.nseindia.com/archives/nsccl/mwpl',
                        'fname':'nseoi_%02d%02d%d',
                        'ext':'.zip',
                        'start_date':datetime(2010,1,2).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "foclipos": {
                        'eg':'https://www.nseindia.com/content/nsccl/mwpl_cli_02012012.xls',
                        'url': 'https://www.nseindia.com/content/nsccl',
                        'fname':'mwpl_cli_%02d%02d%d',
                        'ext':'.xls',
                        'start_date':datetime(2012,1,2).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                
                "comboi": {
                        'eg':'https://www.nseindia.com/archives/nsccl/mwpl/combineoi_01022010.zip',
                        'url': 'https://www.nseindia.com/archives/nsccl/mwpl',
                        'fname':'combineoi_%02d%02d%d',
                        'ext':'.zip',
                        'start_date':datetime(2010,1,2).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "fo_cat_to": {
                        'eg':'https://www.nseindia.com/archives/fo/cat/fo_cat_turnover_090311.xls',
                        'url': 'https://www.nseindia.com/archives/fo/cat',
                        'fname':'fo_cat_turnover_%02d%02d%02d',
                        'ext':'.xls',
                        'start_date':datetime(2011,3,9).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "fo_pvol": {
                        'eg':'https://www.nseindia.com/content/nsccl/fao_participant_vol_02012012.csv',
                        'url': 'https://www.nseindia.com/content/nsccl',
                        'fname':'fao_participant_vol_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2012,2,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "fo_pvol": {
                        'eg':'https://www.nseindia.com/content/nsccl/fao_participant_oi_02012012.csv',
                        'url': 'https://www.nseindia.com/content/nsccl',
                        'fname':'fao_participant_oi_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2012,2,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "fo_t10": {
                        'eg':'https://www.nseindia.com/content/nsccl/fao_top10cm_to_02012012.csv',
                        'url': 'https://www.nseindia.com/content/nsccl',
                        'fname':'fao_top10cm_to_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2012,2,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "fo_derstat": {
                        'eg':'https://www.nseindia.com/content/fo/fii_stats_01-Dec-2014.xls',
                        'url': 'https://www.nseindia.com/content/fo',
                        'fname':'fii_stats_%02d-%s-%d',
                        'ext':'.xls',
                        'start_date':datetime(2014,12,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month].title(),d.year),
                },
                "cdsett": {
                        'eg':'https://www.nseindia.com/archives/cd/sett/CDSett_prce_01092008.csv',
                        'url': 'https://www.nseindia.com/archives/cd/sett',
                        'fname':'CDSett_prce_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2008,9,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "cdvol": {
                        'eg':'https://www.nseindia.com/archives/cd/volt/X_VOLT_29082008.csv',
                        'url': 'https://www.nseindia.com/archives/cd/volt',
                        'fname':'X_VOLT_%02d%02d%d',
                        'ext':'.csv',
                        'start_date':datetime(2008,8,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "cdolim": {
                        'eg':'https://www.nseindia.com/archives/cd/cli/x_oi_cli_limit_29-AUG-2008.lst',
                        'url': 'https://www.nseindia.com/archives/cd/cli',
                        'fname':'x_oi_cli_limit_%02d-%s-%d',
                        'ext':'.lst',
                        'start_date':datetime(2008,8,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "cdtlim": {
                        'eg':'https://www.nseindia.com/archives/cd/cli/x_oi_tm_limit_29-AUG-2008.lst',
                        'url': 'https://www.nseindia.com/archives/cd/cli',
                        'fname':'x_oi_tm_limit_%02d-%s-%d',
                        'ext':'.lst',
                        'start_date':datetime(2008,8,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "cdbhav1": {
                        'eg':'https://www.nseindia.com/archives/cd/bhav/CD_NSEUSDINR290808.dbf.zip',
                        'url': 'https://www.nseindia.com/archives/cd/bhav',
                        'fname':'CD_NSEUSDINR%02d%02d%02d.dbf',
                        'ext':'.zip',
                        'start_date':datetime(2008,8,29).date(),
                        'end_date':datetime(2010,10,27).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "cdbhav2": {
                        'eg':'https://www.nseindia.com/archives/cd/bhav/CD_Bhavcopy291010.zip',
                        'url': 'https://www.nseindia.com/archives/cd/bhav',
                        'fname':'CD_Bhavcopy%02d%02d%02d',
                        'ext':'.zip',
                        'start_date':datetime(2010,10,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "cdmar1": {
                        'eg':'https://www.nseindia.com/archives/cd/mkt_act/cd01092008.doc',
                        'url': 'https://www.nseindia.com/archives/cd/mkt_act',
                        'fname':'cd%02d%02d%d',
                        'ext':'.doc',
                        'start_date':datetime(2008,9,1).date(),
                        'end_date':datetime(2012,9,28).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "cdmar2": {
                        'eg':'https://www.nseindia.com/archives/cd/mkt_act/cd01102012.zip',
                        'url': 'https://www.nseindia.com/archives/cd/mkt_act',
                        'fname':'cd%02d%02d%d',
                        'ext':'.zip',
                        'start_date':datetime(2012,10,1).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "cdbp": {
                        'eg':'https://www.nseindia.com/archives/cd/bp/X_bp290808.csv',
                        'url': 'https://www.nseindia.com/archives/cd/bp',
                        'fname':'X_bp%02d%02d%02d',
                        'ext':'.csv',
                        'start_date':datetime(2008,8,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "cdoiplim": {
                        'eg':'https://www.nseindia.com/archives/cd/cli/x_oi_pro_limit_29-OCT-2014.lst',
                        'url': 'https://www.nseindia.com/archives/cd/cli',
                        'fname':'x_oi_pro_limit_%02d-%s-%d',
                        'ext':'.lst',
                        'start_date':datetime(2014,10,29).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,monthstr[d.month],d.year),
                },
                "bnd_ewpl": {
                        'eg':'https://www.nseindia.com/archives/ird/ewpl/EWPL_08032016.CSV',
                        'url': 'https://www.nseindia.com/archives/ird/ewpl',
                        'fname':'EWPL_%02d%02d%d',
                        'ext':'.CSV',
                        'start_date':datetime(2016,3,8).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year),
                },
                "bndeod1": {
                        'eg':'https://www.nseindia.com/archives/ird/bhav/IRF_NSE310809.dbf.zip',
                        'url': 'https://www.nseindia.com/archives/ird/bhav',
                        'fname':'IRF_NSE%02d%02d%02d.dbf',
                        'ext':'.zip',
                        'start_date':datetime(2009,8,31).date(),
                        'end_date':datetime(2016,2,15).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                "bndeod2": {
                        'eg':'https://www.nseindia.com/archives/ird/bhav/IRF_NSE160216.csv',
                        'url': 'https://www.nseindia.com/archives/ird/bhav',
                        'fname':'IRF_NSE%02d%02d%02d',
                        'ext':'.csv',
                        'start_date':datetime(2016,2,16).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year-2000),
                },
                
                #https://nseindia.com/content/equities/sec_list.csv
                #https://nseindia.com/content/nsccl/CPR.txt
                #https://nseindia.com/content/nsccl/elm.csv
                #https://nseindia.com/content/equities/eq_etfseclist.csv
                
        },
        'bse': {
                
                "eod_stk" : {
                        'eg':'http://www.bseindia.com/download/BhavCopy/Equity/eq230707_csv.zip',
                        'url':'http://www.bseindia.com/download/BhavCopy/Equity',
                        'fname':"EQ%02d%02d%02d",
                        'ext':'_CSV.zip',
                        'start_date':datetime(2007,7,7).date(),
                        'get_url':lambda urlt,d:urlt,
                        'get_file': lambda filet,d: filet%(d.day,d.month,d.year%2000),
                },
                "eod_fo" : {
                        'eg':'http://www.bseindia.com/download/Bhavcopy/Derivative/bhavcopy11-01-08.zip',
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

def store_url_to_file(hdr, url, fname, retry_file, not_found,istoday=False):
        isok=False
        ecode=0
        
        try:
                log.debug( 'Fetching  %s' % url)
                req = Request(url, headers=hdr)
                data=urlopen(req).read()
                                
                with open(fname,"wb") as f:
                        f.write(data)
                        log.info('wrote [%s]' % fname)
                        isok=True
        except urllib.error.HTTPError as e:
                log.error("Error fetching [%s]: %s" % (url, e))
                ecode=e.code
                if ecode == 404:
                        if not istoday:
                                not_found.add(url)
        except:
                log.error("Error fetching [%s]: %s" % (fname, str(sys.exc_info()[0])))

        if not isok and ecode != 404:
                retry_file.write(url + "\n")

        return isok

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
        dirname=os.path.join(curdir, exch, type)
        try:
                os.makedirs(dirname)
        except:
                pass
        not_found_fname=os.path.join(dirname,"not_found.txt")
        retry_fname=os.path.join(dirname,"retry.txt")
        retry_proc_fname=os.path.join(dirname,"retry_proc.txt")
        try:
                os.remove(retry_proc_fname)
        except:
                pass

        prefix="[%s %s]" % (exch,type)
        log.info("%s Starting Fetch" % prefix)

        hdr=hdrmap[exch]
        curmap=sdmap[exch][type]
        start_date=curmap['start_date'] if args.fetch_all else max(curmap['start_date'], args.start_date)
        filet=curmap['fname']
        ext=curmap['ext']
        urlt=curmap['url']
        get_url=curmap['get_url']
        get_file=curmap['get_file']
        freq=curmap["freq"] if "freq" in curmap else None
        
        not_found=set()
        if  args.retry:
                try:
                        os.remove(not_found_fname)
                except:
                        pass
                
        if os.path.exists(not_found_fname):
                with open(not_found_fname ,"r") as f:
                        for l in f:
                                not_found.add(l.rstrip())

        not_found_from_file=len(not_found)
        
        if os.path.exists(retry_fname):
                os.rename(retry_fname, retry_proc_fname)
                with open(retry_fname, "w") as retry_file, open(not_found_fname,"a+") as not_found_file, open(retry_proc_fname,"r") as f :
                        for url in f:
                                url = url.rstrip()
                                fname=os.path.join(dirname, url.split('/')[-1])
                                store_url_to_file(hdr, url, fname, retry_file, not_found)

                os.remove(retry_proc_fname)
                
        if len(not_found) > not_found_from_file:
                with open(not_found_fname ,"w") as f:
                        for l in not_found:
                                f.write(l+"\n")

        not_found_from_file = len(not_found)
        today=datetime.now().date()
        end_date = curmap['end_date']  if 'end_date' in curmap else today

        log.info("%s Fetching from [%s] -> [%s]" % (prefix, str(start_date), str(end_date)))
        last_day=None
        with open(retry_fname,"w") as retry_file:
                for i in list(rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO,TU,WE,TH,FR))):
                        if (i.month, i.day) in holidays:
                                log.debug('%s Skipping holiday: %s' % (prefix, str(i)))
                                continue

                        if (freq is not None) and (last_day is not None) and getattr(last_day, freq) == getattr(i, freq):
                                continue
                                
                        log.debug( '%s Fetching for %s' % (prefix, str(i)))
                        bfname = get_file(filet,i)
                        fname = os.path.join(dirname, bfname+ext)

                        if  fexists(str(os.path.join(dirname,bfname))):
                                log.debug("%s Skipping %s (already exists)" % (prefix, fname))
                                last_day=i
                        else:
                                url = "%s/%s%s" % (get_url(urlt,i), bfname,ext)
                                if  url  in not_found:
                                        log.debug( '%s In not found list, skipping  [%s]' % (prefix, fname))
                                else:
                                        istoday=i.date()==today
                                        if freq=='month':
                                                istoday = i.date().month==today.month
                                        elif freq=='year':
                                                istoday = i.date().year==today.year
                                                
                                        if store_url_to_file(hdr, url, fname, retry_file, not_found, istoday):
                                                last_day=i

        summary.append(("%s [%s]" % (exch, type), str(len(not_found))));
        if len(not_found) > not_found_from_file:
                with open(not_found_fname ,"w") as f:
                        for l in sorted(not_found):
                                f.write(l+"\n")

        if os.path.getsize(retry_fname)==0:
                os.remove(retry_fname)
                
        log.info("%s Finished fetching" % (prefix))

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
                        mon = monthstr.index(p[0])
                else:
                        yr=int(p[0])
                        
                return datetime(yr,mon,dd).date()
        elif lp==2:
                mon = monthstr.index(p[0])
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

def init_logging(args):
        log.setLevel(logging.DEBUG)

        fh = logging.FileHandler('%s.log' % sys.argv[0])
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        if args.log_to_file:
                log.addHandler(fh)
                
        log.addHandler(ch)

def print_data():
        today=datetime.now().date()
        print("Details of Data to be Fetched:")
        for k1, v1 in sdmap.items():
                for k2,v2 in v1.items():
                        filet=v2['fname']
                        ext=v2['ext']
                        urlt=v2['url']
                        get_url=v2['get_url']
                        get_file=v2['get_file']
                        fname = get_file(filet,today)
                        url = "%s/%s%s" % (get_url(urlt,today), fname, ext)

                        print("+%06s+%15s+%6s+%s"%(6*'-',15*'-',6*'-',80*'-'))
                        print("|%05s |%14s |%5s | %s"%(k1,k2,"eg",v2["eg"]))
                        print("|%05s |%14s |%5s | %s"%("","","url",url))
                        print("|%05s |%14s |%5s | %s"%("","","freq",v2["freq"] if "freq" in v2 else "day"))
                        print("|%05s |%14s |%5s | %s"%("","","sdt",v2["start_date"].strftime('%d %b %Y')))
                        print("|%05s |%14s |%5s | %s"%("","","edt",v2["end_date"].strftime('%d %b %Y') if "end_date" in v2 else "(none)"))

        print("+%06s+%15s+%6s+%s"%(6*'-',15*'-',6*'-',80*'-'))

        
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
        parser.add_argument("-r","--retry",action='store_true',help="Delete not found data and retry to download all files again")
        parser.add_argument("-l","--log-to-file",action='store_true',help="Enable verbose logging to log file")
        parser.add_argument("-p","--print-data",action='store_true',help="Print table of preconfigured data for all known sources")
        parser.add_argument("-t","--use-threads",action='store_true',help="Fetch the data using multiple threads")
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

        init_logging(args)

        if args.print_data:
                print_data()
                sys.exit(0)
                
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

        if args.use_threads:
                log.info("Using multithreaded fetch")
                threads=[]
                for k1, v1 in sdmap.items():
                        for k2,v2 in v1.items():
                                thr=Thread(target=fetch_files,args=(k1, k2, args))
                                threads.append(thr)
                                thr.start()
                                
                for t in threads:
                        t.join()
        else:
                for k1, v1 in sdmap.items():
                        for k2,v2 in v1.items():
                                fetch_files(k1, k2, args)
                        
        log.info("All fetch complete")
        log.debug(print_table(summary))

