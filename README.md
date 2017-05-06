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
* NSE
  * Equities EOD (eg. cm19APR2017bhav.csv.zip)
  * Futures, Options EOD(eg.fo19APR2017bhav.csv.zip)
  * Volatility (eg. CMVOLT_28032011.CSV)
  * Market Activity (eg. MA200417.csv)
  * Maket Turnover (eg. MTO_01012002.DAT)
  * Margin trading (eg. eg Margintrdg_200417.zip)
  * Category turnover (eg. cat_turnover_200417.xls)
  * Short selling data (eg. shortselling_17072012.csv)

  * **Not yet:**
    * Combined Report(eg. combined_report20042017.zip)
    * Variance reports (eg. C_VAR1_19042017_[1-6].DAT)

* BSE
  * Equities (eq230707_csv.zip)
  * Futures, options (bhavcopy11-01-08.zip)

*Running the script*
```
usage: fetchbhavcopy.py [-h] [-d DUMP_DIR] [-a] [-s START_DATE]

Fetch bhavcopy archives from NSE/BSE.
    By default, only files from last 2 weeks are fetched - can be overridden by  switches below.
    Typical usage is download all data for the first time and then use default (last 2 weeks only) 
    to speed up the fetch

Sample usage:
  fetchbhavcopy.py -d ..\data\dumpshavcopy -s "20 apr 2017"
  fetchbhavcopy.py -d ..\data\dumpshavcopy -s "20 APR" #downloads from given date of current year
  fetchbhavcopy.py -d ..\data\dumpshavcopy -s 2015 #from jan 1 2015
  fetchbhavcopy.py -d ..\data\dumpshavcopy -s feb #from feb 1 of current year, case of month string doesnt matter
  fetchbhavcopy.py -d ..\data\dumpshavcopy -a #all available data - 20+ years for equities
  fetchbhavcopy.py -d ..\data\dumpshavcopy  #for last 2 weeks - default
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