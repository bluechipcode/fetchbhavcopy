(in-package :downloader)


;;exhange: which exchange, code: code for the script. open,high,low,close,prevclose,tottrdqty,: self explanatory
;;last: last traded price (might be diff  from close price)
;;dndratio: delivery to non-delivery ratio
;;type: one of equity, debenture, preference, bond: default: equity
;;group: classification of the instrument in the given exchange
(clsql:def-view-class eod-data ()
  ((date 		:initarg :date 		:type (string 20) :db-type "DATE")  
   (exchange    :initarg :exchange 	:type (string 20)) 
   (code 		:initarg :code 		:type (string 20)) 
   (open 		:initarg :open 		:type float :db-type "NUMERIC 30,2")
   (high 		:initarg :high 		:type float :db-type "NUMERIC 30,2") 
   (low 		:initarg :low 		:type float :db-type "NUMERIC 30,2") 
   (close 		:initarg :close 	:type float :db-type "NUMERIC 30,2") 
   (last 		:initarg :last 		:type float :db-type "NUMERIC 30,2") 
   (prevclose 	:initarg :prevclose :type float :db-type "NUMERIC 30,2") 
   (tottrdqty 	:initarg :tottrdqty :type integer :db-type "NUMERIC 30") 
   (dndratio 	:initarg :dndratio 	:type float :db-type   "NUMERIC 7,4") 
   (type 		:initarg :type 		:type (string 20))  
   (grp 		:initarg :grp 		:type (string 20))) ;;name cant be group, as group is a SQL keyword                    
   (:base-table eod_data))
  


;; Represents an eod data source
  (defclass eod-data-source () ())


(defgeneric update-eod-data (eod-data-source date db tblname)
  (:documentation "Update the database with the eod data"))

;;The following are the methods which need to be implemented by every data source
(defgeneric get-url (eod-data-source date)
  (:documentation "Get the url for given date and eod source"))

;;eod-data-list is the current list
;;for bse, date is not in the raw data, so date is always passed
(defgeneric process-raw-data (eod-data-source raw-data date eod-data-list ) 
  (:documentation "Process the raw data to produce list of eod-data"))


;;common implementation in the base class
(defmethod update-eod-data ((eod-data-source eod-data-source) date db tblname)
  (let* ((url (get-url eod-data-source date))
		 (raw-data (drakma:http-request url))
		 (eod-data-list)
		 (processed-data (process-raw-data eod-data-source raw-data date eod-data-list)))

	;;insert the processed data
	(mapcar #'(lambda (rec)
				(clsql:update-records-from-instance rec :database db))
			proncessed-data)

	(format t "Updated the eod data~%")))
  
  
;;NSE
;; http://www.nseindia.com/content/historical/EQUITIES/2002/JAN/cm01JAN2002bhav.csv
(defclass nse-eod-data-source (eod-data-source) ())

(defmethod get-url ((eod-data-source nse-eod-data-source) date)
  (multiple-value-bind
		(second minute hour date month year day-of-week dst-p tz)
	  (decode-universal-time date)
	(declare (ignore sec min dowk dst-p tz))
	(let ((monthstr (format nil "~[JAN~;FEB~;MAR~;APR~;MAY~;JUN~;JUL~;AUG~;SEP~;OCT~;NOV~;DEC~]" (- month 1))))
	  (format nil "http://www.nseindia.com/content/historical/EQUITIES/~4D/~A/cm~2,'0D~A~4Dbhav.csv" 
			  year monthstr date monthstr year))))

(defun get-date-str (date)
  (multiple-value-bind
		(sec min hr date mon year dowk dst-p tz)
	  (decode-universal-time date)
	(declare (ignore sec min dowk dst-p tz))
	(format nil "~2,'0d ~[JAN~;FEB~;MAR~;APR~;MAY~;JUN~;JUL~;AUG~;SEP~;OCT~;NOV~;DEC~] ~4,'0d" date  (- mon 1) year)))


(defun process-raw-data-helper (raw-data item-map exch eod-data-list &optional date)
  (let ((seq (cdr (cl-ppcre:split "\\n" raw-data)))) ;;ignore first line of csv
	(dolist (line seq eod-data-list)
	  (let* ((items (cl-ppcre:split "," line))
			(datestr (if date
						 (get-date-str date)
						 (nth (aref item-map 9) items))))
		(push (make-instance 'eod-data 
			   :code      (nth (aref item-map 0) items)
			   :type      (nth (aref item-map 1) items) 
			   :open      (float (read-from-string (nth (aref item-map 2) items)))
			   :high      (float (read-from-string (nth (aref item-map 3) items)))
			   :low       (float (read-from-string (nth (aref item-map 4) items)))
			   :close     (float (read-from-string (nth (aref item-map 5) items)))
			   :last      (float (read-from-string (nth (aref item-map 6) items)))
			   :prevclose (float (read-from-string (nth (aref item-map 7) items)))
			   :tottrdqty (parse-integer (nth (aref item-map 8) items) :junk-allowed t)
			   :date      datestr
			   :exchange  exch)
			  eod-data-list)))))

;;what is series: http://www.nseindia.com/content/equities/eq_serieslist.htm
;;header: "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,"
(defmethod process-raw-data ((eod-data-source nse-eod-data-source) raw-data date eod-data-list)
  (process-raw-data-helper raw-data '#(0 1 2 3 4 5 6 7 8 10) "NSE" eod-data-list))


;;BSE
;;http://www.bseindia.com/bhavcopy/eq011008_csv.zip
(defclass bse-eod-data-source (eod-data-source) ())

(defmethod get-url ((eod-data-source bse-eod-data-source) date)
  (multiple-value-bind
		(second minute hour date month year day-of-week dst-p tz)
	  (decode-universal-time date)
	(format nil "http://www.bseindia.com/bhavcopy/eq~2,'0d~2,'0d~2,'0d_csv.zip"
			date month (mod year 100))))

(defun unzip (buf)
  ;;the magic number 42 is the offset of zip data in one file zip files without comment
  (setf buf (make-array (- (length buf) 42) :displaced-to buf :displaced-index-offset 42 :element-type '(unsigned-byte 8)))
  (let ((in (flexi-streams:make-in-memory-input-stream buf))
		(out (flexi-streams:make-in-memory-output-stream)))
	(util.zip:inflate in out)
	(flexi-streams:octets-to-string (flexi-streams:get-output-stream-sequence out))))

(defmethod process-raw-data ((eod-data-source bse-eod-data-source) raw-data date eod-data-list)
  (process-raw-data-helper (unzip raw-data) '#(0 2 4 5 6 7 8 9 12) "BSE" eod-data-list date))

