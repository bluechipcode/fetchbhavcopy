# fetchbhavcopy
Fetches EOD files from NSE/BSE bhavcopy and dumps the files locally. The script is built to enable download history and keep the EOD dump updated. First run will fetch the entire history till date and the subsequent ones will update whatever is missing. At every run all missing files till date will be downloaded and the dump will be updated with latest files as of run date.

As long as you pass the same dump dir to every invocation (there is a default, which you can choose to override) the script will update the dump with missing files and not redownload existing files. 

* Has a basic holiday calendar - weekends and well known Indian public holidays (Independence day, Gandhi Jayanthi etc whose dates dont change)
* Can recognize any missing files by adding the 404'd files to not_found list so as to not fetch them again when run next time
* Can retry timed out files
* Can recognize unzipped files - so you can unzip the bhavcopy in place and use them. The script will recognize them and not refetch
* Stores missing, retry files under not_found.txt, retry.txt files in each folder so you can manually choose to edit
* Just delete the not found, retry files to prod the script to try download the not found scripts again - not needed generally


**Files fetched:**
```
Details of Data to be Fetched:
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       eod_stk |   eg | https://www.nseindia.com/content/historical/EQUITIES/2017/APR/cm19APR2017bhav.csv.zip
|      |               |  url | https://www.nseindia.com/content/historical/EQUITIES/2017/MAY/cm18MAY2017bhav.csv.zip
|      |               | freq | day
|      |               |  sdt | 03 Nov 1994
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        eod_fo |   eg | https://www.nseindia.com/content/historical/DERIVATIVES/2008/JAN/fo22JAN2008bhav.csv.zip
|      |               |  url | https://www.nseindia.com/content/historical/DERIVATIVES/2017/MAY/fo18MAY2017bhav.csv.zip
|      |               | freq | day
|      |               |  sdt | 12 Jun 2000
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       eod_mto |   eg | https://www.nseindia.com/archives/equities/mto/MTO_01012002.DAT
|      |               |  url | https://www.nseindia.com/archives/equities/mto/MTO_18052017.DAT
|      |               | freq | day
|      |               |  sdt | 01 Jan 2002
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse | eod_shortsell |   eg | https://www.nseindia.com/archives/equities/shortSelling/shortselling_17072012.csv
|      |               |  url | https://www.nseindia.com/archives/equities/shortSelling/shortselling_18052017.csv
|      |               | freq | day
|      |               |  sdt | 17 Jul 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       eod_vol |   eg | https://www.nseindia.com/archives/nsccl/volt/CMVOLT_28032011.CSV
|      |               |  url | https://www.nseindia.com/archives/nsccl/volt/CMVOLT_18052017.CSV
|      |               | freq | day
|      |               |  sdt | 17 Apr 2011
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |     ind_close |   eg | https://nseindia.com/content/indices/ind_close_all_09052017.csv
|      |               |  url | https://nseindia.com/content/indices/ind_close_all_18052017.csv
|      |               | freq | day
|      |               |  sdt | 21 Feb 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |     margintrd |   eg | https://www.nseindia.com/archives/equities/margin/Margintrdg_130404.zip
|      |               |  url | https://www.nseindia.com/archives/equities/margin/Margintrdg_180517.zip
|      |               | freq | day
|      |               |  sdt | 13 Apr 2004
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cat_to |   eg | https://www.nseindia.com/archives/equities/cat/cat_turnover_110405.xls
|      |               |  url | https://www.nseindia.com/archives/equities/cat/cat_turnover_180517.xls
|      |               | freq | day
|      |               |  sdt | 11 Apr 2005
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       combrep |   eg | https://www.nseindia.com/archives/combine_report/combined_report06082012.zip
|      |               |  url | https://www.nseindia.com/archives/combine_report/combined_report18052017.zip
|      |               | freq | day
|      |               |  sdt | 06 Aug 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      seccateg |   eg | https://www.nseindia.com/archives/nsccl/mult/C_CATG_APR2003.T01
|      |               |  url | https://www.nseindia.com/archives/nsccl/mult/C_CATG_JUN2017.T01
|      |               | freq | month
|      |               |  sdt | 06 Aug 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |          mcwb |   eg | https://www.nseindia.com/content/indices/mcwb_jan08.zip
|      |               |  url | https://www.nseindia.com/content/indices/mcwb_jun17.zip
|      |               | freq | month
|      |               |  sdt | 01 Jan 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      imp_cost |   eg | https://nseindia.com/content/indices/ind_ic_2001.zip
|      |               |  url | https://nseindia.com/content/indices/ind_ic_2017.zip
|      |               | freq | year
|      |               |  sdt | 01 Jan 2001
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      clifund1 |   eg | https://www.nseindia.com/archives/equities/client/cli_fund_jul06.csv
|      |               |  url | https://www.nseindia.com/archives/equities/client/cli_fund_jun17.csv
|      |               | freq | day
|      |               |  sdt | 01 Jul 2006
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       ind_mcw |   eg | https://nseindia.com/content/indices/indices_dataApr2013.zip
|      |               |  url | https://nseindia.com/content/indices/indices_dataJun2017.zip
|      |               | freq | month
|      |               |  sdt | 01 Apr 2014
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |     fo_secban |   eg | https://nseindia.com/archives/fo/sec_ban/fo_secban_01012008.csv
|      |               |  url | https://nseindia.com/archives/fo/sec_ban/fo_secban_18052017.csv
|      |               | freq | day
|      |               |  sdt | 01 Jan 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |          mar1 |   eg | https://www.nseindia.com/archives/equities/mkt/mkt03012000.doc
|      |               |  url | https://www.nseindia.com/archives/equities/mkt/mkt18052017.doc
|      |               | freq | day
|      |               |  sdt | 03 Jan 2000
|      |               |  edt | 17 Feb 2012
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |          mar2 |   eg | https://www.nseindia.com/archives/equities/mkt/MA200417.csv
|      |               |  url | https://www.nseindia.com/archives/equities/mkt/MA180517.csv
|      |               | freq | day
|      |               |  sdt | 21 Feb 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       fo_rep1 |   eg | https://www.nseindia.com/archives/fo/mkt/fo050701.doc
|      |               |  url | https://www.nseindia.com/archives/fo/mkt/fo180517.doc
|      |               | freq | day
|      |               |  sdt | 05 Jul 2001
|      |               |  edt | 22 Feb 2011
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       fo_rep2 |   eg | https://www.nseindia.com/archives/fo/mkt/fo23022011.ZIP
|      |               |  url | https://www.nseindia.com/archives/fo/mkt/fo18052017.zip
|      |               | freq | day
|      |               |  sdt | 23 Feb 2011
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |   fosett_prce |   eg | https://nseindia.com/archives/nsccl/sett/FOSett_prce_19102001.csv
|      |               |  url | https://nseindia.com/archives/nsccl/sett/FOSett_prce_18052017.csv
|      |               | freq | day
|      |               |  sdt | 19 Oct 2001
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |  cli_poslimit |   eg | https://nseindia.com/archives/nsccl/cli/oi_cli_limit_02-JAN-2002.lst
|      |               |  url | https://nseindia.com/archives/nsccl/cli/oi_cli_limit_18-MAY-2017.lst
|      |               | freq | day
|      |               |  sdt | 02 Jan 2002
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |         nseoi |   eg | https://www.nseindia.com/archives/nsccl/mwpl/nseoi_01022010.zip
|      |               |  url | https://www.nseindia.com/archives/nsccl/mwpl/nseoi_18052017.zip
|      |               | freq | day
|      |               |  sdt | 02 Jan 2010
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      foclipos |   eg | https://www.nseindia.com/content/nsccl/mwpl_cli_02012012.xls
|      |               |  url | https://www.nseindia.com/content/nsccl/mwpl_cli_18052017.xls
|      |               | freq | day
|      |               |  sdt | 02 Jan 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        comboi |   eg | https://www.nseindia.com/archives/nsccl/mwpl/combineoi_01022010.zip
|      |               |  url | https://www.nseindia.com/archives/nsccl/mwpl/combineoi_18052017.zip
|      |               | freq | day
|      |               |  sdt | 02 Jan 2010
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |     fo_cat_to |   eg | https://www.nseindia.com/archives/fo/cat/fo_cat_turnover_090311.xls
|      |               |  url | https://www.nseindia.com/archives/fo/cat/fo_cat_turnover_180517.xls
|      |               | freq | day
|      |               |  sdt | 09 Mar 2011
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       fo_pvol |   eg | https://www.nseindia.com/content/nsccl/fao_participant_oi_02012012.csv
|      |               |  url | https://www.nseindia.com/content/nsccl/fao_participant_oi_18052017.csv
|      |               | freq | day
|      |               |  sdt | 01 Feb 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        fo_t10 |   eg | https://www.nseindia.com/content/nsccl/fao_top10cm_to_02012012.csv
|      |               |  url | https://www.nseindia.com/content/nsccl/fao_top10cm_to_18052017.csv
|      |               | freq | day
|      |               |  sdt | 01 Feb 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |    fo_derstat |   eg | https://www.nseindia.com/content/fo/fii_stats_01-Dec-2014.xls
|      |               |  url | https://www.nseindia.com/content/fo/fii_stats_18-Jun-2017.xls
|      |               | freq | day
|      |               |  sdt | 01 Dec 2014
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cdsett |   eg | https://www.nseindia.com/archives/cd/sett/CDSett_prce_01092008.csv
|      |               |  url | https://www.nseindia.com/archives/cd/sett/CDSett_prce_18052017.csv
|      |               | freq | day
|      |               |  sdt | 01 Sep 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |         cdvol |   eg | https://www.nseindia.com/archives/cd/volt/X_VOLT_29082008.csv
|      |               |  url | https://www.nseindia.com/archives/cd/volt/X_VOLT_18052017.csv
|      |               | freq | day
|      |               |  sdt | 29 Aug 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cdolim |   eg | https://www.nseindia.com/archives/cd/cli/x_oi_cli_limit_29-AUG-2008.lst
|      |               |  url | https://www.nseindia.com/archives/cd/cli/x_oi_cli_limit_18-JUN-2017.lst
|      |               | freq | day
|      |               |  sdt | 29 Aug 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cdtlim |   eg | https://www.nseindia.com/archives/cd/cli/x_oi_tm_limit_29-AUG-2008.lst
|      |               |  url | https://www.nseindia.com/archives/cd/cli/x_oi_tm_limit_18-JUN-2017.lst
|      |               | freq | day
|      |               |  sdt | 29 Aug 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       cdbhav1 |   eg | https://www.nseindia.com/archives/cd/bhav/CD_NSEUSDINR290808.dbf.zip
|      |               |  url | https://www.nseindia.com/archives/cd/bhav/CD_NSEUSDINR180517.dbf.zip
|      |               | freq | day
|      |               |  sdt | 29 Aug 2008
|      |               |  edt | 27 Oct 2010
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       cdbhav2 |   eg | https://www.nseindia.com/archives/cd/bhav/CD_Bhavcopy291010.zip
|      |               |  url | https://www.nseindia.com/archives/cd/bhav/CD_Bhavcopy180517.zip
|      |               | freq | day
|      |               |  sdt | 29 Oct 2010
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cdmar1 |   eg | https://www.nseindia.com/archives/cd/mkt_act/cd01092008.doc
|      |               |  url | https://www.nseindia.com/archives/cd/mkt_act/cd18052017.doc
|      |               | freq | day
|      |               |  sdt | 01 Sep 2008
|      |               |  edt | 28 Sep 2012
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |        cdmar2 |   eg | https://www.nseindia.com/archives/cd/mkt_act/cd01102012.zip
|      |               |  url | https://www.nseindia.com/archives/cd/mkt_act/cd18052017.zip
|      |               | freq | day
|      |               |  sdt | 01 Oct 2012
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |          cdbp |   eg | https://www.nseindia.com/archives/cd/bp/X_bp290808.csv
|      |               |  url | https://www.nseindia.com/archives/cd/bp/X_bp180517.csv
|      |               | freq | day
|      |               |  sdt | 29 Aug 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      cdoiplim |   eg | https://www.nseindia.com/archives/cd/cli/x_oi_pro_limit_29-OCT-2014.lst
|      |               |  url | https://www.nseindia.com/archives/cd/cli/x_oi_pro_limit_18-JUN-2017.lst
|      |               | freq | day
|      |               |  sdt | 29 Oct 2014
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |      bnd_ewpl |   eg | https://www.nseindia.com/archives/ird/ewpl/EWPL_08032016.CSV
|      |               |  url | https://www.nseindia.com/archives/ird/ewpl/EWPL_18052017.CSV
|      |               | freq | day
|      |               |  sdt | 08 Mar 2016
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       bndeod1 |   eg | https://www.nseindia.com/archives/ird/bhav/IRF_NSE310809.dbf.zip
|      |               |  url | https://www.nseindia.com/archives/ird/bhav/IRF_NSE180517.dbf.zip
|      |               | freq | day
|      |               |  sdt | 31 Aug 2009
|      |               |  edt | 15 Feb 2016
+------+---------------+------+--------------------------------------------------------------------------------
|  nse |       bndeod2 |   eg | https://www.nseindia.com/archives/ird/bhav/IRF_NSE160216.csv
|      |               |  url | https://www.nseindia.com/archives/ird/bhav/IRF_NSE180517.csv
|      |               | freq | day
|      |               |  sdt | 16 Feb 2016
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  bse |       eod_stk |   eg | http://www.bseindia.com/download/BhavCopy/Equity/eq230707_csv.zip
|      |               |  url | http://www.bseindia.com/download/BhavCopy/Equity/EQ180517_CSV.zip
|      |               | freq | day
|      |               |  sdt | 07 Jul 2007
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------
|  bse |        eod_fo |   eg | http://www.bseindia.com/download/Bhavcopy/Derivative/bhavcopy11-01-08.zip
|      |               |  url | http://www.bseindia.com/download/Bhavcopy/Derivative/bhavcopy18-05-17.zip
|      |               | freq | day
|      |               |  sdt | 11 Jan 2008
|      |               |  edt | (none)
+------+---------------+------+--------------------------------------------------------------------------------

```
## Running the script
```
usage: fetchbhavcopy.py [-h] [-d DUMP_DIR] [-a] [-s START_DATE]

Fetch bhavcopy archives from NSE/BSE.
    By default, only files from last 2 weeks are fetched - can be overridden by  switches below.
    Typical usage is to download all data for the first time and then use default (last 2 weeks only) 
    to speed up the fetch

Sample usage:
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s "20 apr 2017"
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s "20 APR" #downloads from given date of current year
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s 2015 #from jan 1 2015
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -s feb #from feb 1 of current year, case of month string doesnt matter
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy -a #all available data - 20+ years for equities
  fetchbhavcopy.py -d ..\data\dumps\bhavcopy  #for last 2 weeks - default
  fetchbhavcopy.py   #for last 2 weeks and use default dump directory

optional arguments:
  -h, --help            show this help message and exit
  -d DUMP_DIR, --dump-dir DUMP_DIR
                        Directory to dump the files to (default: dumps/bhavcopy)
  -a, --fetch-all       Download from start of history (default: False)
  -s START_DATE, --start-date START_DATE
                        Specify a starting date 
                        Any valid year can be specified, but actual year used depends on data available 
                        Eg. NSE equities bhavcopy is available only from 1994 so year=max(year specified, 1994)
                        Accepted formats:
                        * yyyy (From 1 JAN of given year, eg. 1999)
                        * mon yyyy (From day 1, eg. JAN 1999)
                        * dd mon yyyy (eg. 12 JAN 1999)
                        * mon (From current year, day 1 of given month, eg. JAN)
                         (default: 2017-04-22)
```