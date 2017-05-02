;;downloader module of server
(asdf:defsystem #:downloader
  :author "Venkatesan"
  :version "0.1"
  :depends-on ( :drakma
				:flexi-streams
				:clsql
				:cl-ppcre
				:inflate)
  :components ((:file "package")
			   (:file "eod-data-source" 
					  :depends-on ("package"))))





