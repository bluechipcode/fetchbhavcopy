# fetchbhavcopy
Fetch EOD files from NSE/BSE bhavcopy and dumps the files locally. The script is built to enable downloading and maintaining the EOD files upto date. First run will fetch the entire history till date and the subsequent ones will update whatever is missing.

As long as you pass the same dump dir to every invocation (there is a default, which you can choose to override) the script will update the dump with missing files and not redownload existing files. 

* Has a basic holiday calendar - weekends and well known Indian public holidays (Independence day, Gandhi Jayanthi etc whose dates dont change)
* Can recognize any missing files by adding the 404'd files to not_found list so as to not fetch them again when run next time
* Can retry timed out files
* Can recognize unzipped files - so you can unzip the bhavcopy in place and use them
* Stores missing, retry files under not_found.txt, retry.txt files in each folder so you can manually choose to edit
* Just delete the not found, retry files to prod the script to try download the not found scripts again - not needed generally



