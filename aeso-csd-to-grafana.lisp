;;;; aeso-csd-to-grafana.lisp
;;;;
;;;; This code was written by Claude (https://claude.ai), Anthropic's AI assistant,
;;;; through an iterative conversation with:
;;;;
;;;;   Sean Watkins
;;;;   sean.watkins@gmail.com
;;;;   https://www.linkedin.com/in/sean-w-b981934/
;;;;   https://www.strava.com/athletes/35611001
;;;;
;;;; Sean provided the requirements, tested each change against live systems, and guided
;;;; development through real error output and debugging sessions. Every bug fix, API path
;;;; discovery, SSL workaround, and MQTT implementation was developed collaboratively —
;;;; Sean ran the code, the AI wrote it.
;;;;
;;;; Fetches the AESO Current Supply & Demand (CSD) and System Marginal Price (SMP)
;;;; APIs every 60 seconds and writes metrics to InfluxDB (Grafana time-series backend).
;;;; Also publishes MQTT notifications on startup, each poll, and on failure.
;;;;
;;;; AESO endpoints:
;;;;   CSD: https://apimgw.aeso.ca/public/currentsupplydemand-api/v1/csd/generation/assets/current
;;;;   SMP: https://apimgw.aeso.ca/public/systemmarginalprice-api/v1.1/price/systemMarginalPrice/current
;;;;
;;;; Dependencies (Quicklisp):
;;;;   dexador    – HTTP client
;;;;   yason      – JSON parser
;;;;   local-time – Timestamp handling / UTC conversion
;;;;   usocket    – TCP sockets (MQTT + DNS error handling)
;;;;   cl+ssl     – TLS for AESO APIM (Azure APIM close_notify workaround)
;;;;   babel      – UTF-8 encoding for MQTT packets
;;;;
;;;; Usage:
;;;;   sbcl --load aeso-csd-to-grafana.lisp
;;;;
;;;; All configuration is via environment variables or defparameter forms.
;;;; See README.md for full documentation.

;;; ──────────────────────────────────────────────────────────────────
;;; 1.  Bootstrap Quicklisp (edit path if your ql install differs)
;;; ──────────────────────────────────────────────────────────────────

(let ((ql-init (merge-pathnames "quicklisp/setup.lisp"
                                (user-homedir-pathname))))
  (when (probe-file ql-init)
    (load ql-init)))

(ql:quickload '(:dexador :yason :local-time :usocket :cl+ssl :babel) :silent t)

;;; ──────────────────────────────────────────────────────────────────
;;; 2.  Configuration  – edit these before running
;;; ──────────────────────────────────────────────────────────────────

(defparameter *aeso-api-key*
  (or (uiop:getenv "AESO_API_KEY") "YOUR_AESO_API_KEY_HERE")
  "AESO Azure APIM subscription key.
   Obtain one at https://developer-apim.aeso.ca/
   Best practice: export AESO_API_KEY=<key> in your shell.")

;;; AESO CSD endpoint
(defparameter *aeso-csd-url*
  "https://apimgw.aeso.ca/public/currentsupplydemand-api/v1/csd/generation/assets/current"
  "AESO Current Supply & Demand assets endpoint (v1).")

;;; AESO Pool Price / SMP endpoint
(defparameter *aeso-smp-url*
  "https://apimgw.aeso.ca/public/systemmarginalprice-api/v1.1/price/systemMarginalPrice/current"
  "AESO System Marginal Price API (v1.1) — returns current SMP and volume.")

;;; InfluxDB connection
(defparameter *influx-url*
  (or (uiop:getenv "INFLUX_URL") "http://localhost:8086")
  "InfluxDB base URL.")

(defparameter *influx-org*
  (or (uiop:getenv "INFLUX_ORG") "my-org")
  "InfluxDB organisation (v2) or leave empty for v1.")

(defparameter *influx-bucket*
  (or (uiop:getenv "INFLUX_BUCKET") "aeso")
  "InfluxDB bucket (v2) / database (v1).")

(defparameter *influx-token*
  (or (uiop:getenv "INFLUX_TOKEN") "YOUR_INFLUXDB_TOKEN_HERE")
  "InfluxDB v2 API token (or username:password for v1).")

;;; Set to NIL to run once and exit; set to a positive integer to poll.
(defparameter *poll-interval-seconds* 60
  "Seconds between polls.  NIL = run once.")

;;; Set to T temporarily to print raw JSON keys for debugging.
(defparameter *debug-json* nil
  "When T, print raw JSON structure to help diagnose key names.")

;;; ── MQTT configuration ────────────────────────────────────────────
(defparameter *mqtt-host*
  (or (uiop:getenv "MQTT_HOST") "localhost")
  "MQTT broker hostname or IP.")

(defparameter *mqtt-port*
  (parse-integer (or (uiop:getenv "MQTT_PORT") "1883"))
  "MQTT broker port (default 1883).")

(defparameter *mqtt-username*
  (or (uiop:getenv "MQTT_USER") "")
  "MQTT username, or empty string for no auth.")

(defparameter *mqtt-password*
  (or (uiop:getenv "MQTT_PASS") "")
  "MQTT password, or empty string for no auth.")

(defparameter *mqtt-client-id* "aeso-csd-collector"
  "MQTT client identifier.")

(defparameter *mqtt-topic-prefix* "aeso/collector"
  "Base topic prefix. Messages are published to:
     <prefix>/status    – online/offline/error
     <prefix>/poll      – JSON summary after each successful poll
     <prefix>/error     – error details on fetch or write failure")

;;; Tracks consecutive failures for MQTT flag publishing.
(defparameter *consecutive-failures* 0
  "Count of consecutive poll failures. Reset to 0 on success.")

;;; ──────────────────────────────────────────────────────────────────
;;; 3.  Utility helpers
;;; ──────────────────────────────────────────────────────────────────

(defun now-unix-ns ()
  "Return current UTC time as Unix nanoseconds as a plain integer (InfluxDB line-protocol).
   Uses local-time:timestamp-to-unix for an exact integer second count, then adds
   the sub-second nanoseconds, keeping everything in integer arithmetic throughout."
  (let* ((now  (local-time:now))
         (secs (local-time:timestamp-to-unix now))
         (nsec (local-time:nsec-of now)))
    (+ (* secs 1000000000) nsec)))

(defun escape-lp-string (s)
  "Escape special chars in an InfluxDB line-protocol tag/field string value."
  (with-output-to-string (out)
    (loop for ch across (string s)
          do (when (member ch '(#\, #\= #\Space #\" #\\))
               (write-char #\\ out))
             (write-char ch out))))

(defun parse-float (s)
  "Parse a number from string S, returning NIL on error or if not numeric."
  (let ((val (ignore-errors
               (with-input-from-string (in s)
                 (read in)))))
    (when (numberp val) val)))

(defun safe-number (val)
  "Coerce VAL to a float string, returning NIL if not numeric."
  (typecase val
    (number (format nil "~,4F" (float val)))
    (string (let ((n (parse-float val)))
              (when n (format nil "~,4F" (float n)))))
    (t nil)))

;;; ──────────────────────────────────────────────────────────────────
;;; 4.  MQTT client (minimal MQTT 3.1.1 over raw TCP via usocket)
;;;
;;;  Implements just enough of the MQTT 3.1.1 spec to:
;;;    - CONNECT with optional username/password
;;;    - PUBLISH QoS 0 messages
;;;    - DISCONNECT cleanly
;;;
;;;  No external MQTT library required — usocket is already loaded.
;;; ──────────────────────────────────────────────────────────────────

(defun mqtt-encode-string (s)
  "Encode string S as MQTT UTF-8 prefixed string (2-byte length + bytes)."
  (let* ((bytes (babel:string-to-octets s :encoding :utf-8))
         (len   (length bytes))
         (buf   (make-array (+ 2 len) :element-type '(unsigned-byte 8))))
    (setf (aref buf 0) (ash len -8)
          (aref buf 1) (logand len #xFF))
    (replace buf bytes :start1 2)
    buf))

(defun mqtt-encode-remaining-length (n)
  "Encode integer N as MQTT variable-length remaining-length field."
  (let ((result '()))
    (loop do
      (let ((byte (logand n #x7F)))
        (setf n (ash n -7))
        (when (> n 0) (setf byte (logior byte #x80)))
        (push byte result))
      while (> n 0))
    (coerce (nreverse result) '(vector (unsigned-byte 8)))))

(defun mqtt-connect-packet (client-id username password)
  "Build an MQTT 3.1.1 CONNECT packet."
  (let* ((protocol-name  (mqtt-encode-string "MQTT"))
         (protocol-level #x04)           ; MQTT 3.1.1
         (connect-flags  (logior
                           #x02          ; Clean session
                           (if (and username (not (equal username ""))) #xC0 0)))
                                         ; Username + password flags
         (keepalive-msb  0)
         (keepalive-lsb  60)
         (client-id-enc  (mqtt-encode-string client-id))
         (payload        (if (and username (not (equal username "")))
                             (concatenate '(vector (unsigned-byte 8))
                                          client-id-enc
                                          (mqtt-encode-string username)
                                          (mqtt-encode-string password))
                             client-id-enc))
         (var-header     (concatenate '(vector (unsigned-byte 8))
                                      protocol-name
                                      (vector protocol-level connect-flags
                                              keepalive-msb keepalive-lsb)))
         (remaining      (+ (length var-header) (length payload)))
         (rem-enc        (mqtt-encode-remaining-length remaining)))
    (concatenate '(vector (unsigned-byte 8))
                 (vector #x10)           ; CONNECT fixed header
                 rem-enc
                 var-header
                 payload)))

(defun mqtt-publish-packet (topic payload-string)
  "Build an MQTT 3.1.1 PUBLISH packet (QoS 0, no retain)."
  (let* ((topic-enc   (mqtt-encode-string topic))
         (payload-enc (babel:string-to-octets payload-string :encoding :utf-8))
         (remaining   (+ (length topic-enc) (length payload-enc)))
         (rem-enc     (mqtt-encode-remaining-length remaining)))
    (concatenate '(vector (unsigned-byte 8))
                 (vector #x30)           ; PUBLISH, QoS 0, no retain
                 rem-enc
                 topic-enc
                 payload-enc)))

(defun mqtt-disconnect-packet ()
  "Build an MQTT 3.1.1 DISCONNECT packet."
  (vector #xE0 #x00))

;;; Persistent MQTT connection — one socket shared by all threads.
;;; The heartbeat publishes every second so we must not open/close per message.
(defvar *mqtt-socket* nil "Persistent MQTT TCP socket.")
(defvar *mqtt-stream* nil "Stream for *mqtt-socket*.")
(defvar *mqtt-lock*   (sb-thread:make-mutex :name "mqtt-lock")
  "Mutex protecting the shared MQTT socket from concurrent writes.")

(defun mqtt-connect ()
  "Open a persistent connection to the MQTT broker and send CONNECT.
   Stores the socket in *mqtt-socket* and stream in *mqtt-stream*.
   Returns T on success, NIL on failure."
  (handler-case
      (progn
        (when *mqtt-socket*
          (ignore-errors (usocket:socket-close *mqtt-socket*))
          (setf *mqtt-socket* nil *mqtt-stream* nil))
        (let* ((sock   (usocket:socket-connect *mqtt-host* *mqtt-port*
                                               :element-type '(unsigned-byte 8)))
               (stream (usocket:socket-stream sock)))
          (write-sequence (mqtt-connect-packet *mqtt-client-id*
                                               *mqtt-username*
                                               *mqtt-password*)
                          stream)
          (force-output stream)
          ;; Read CONNACK
          (let ((connack (make-array 4 :element-type '(unsigned-byte 8))))
            (read-sequence connack stream)
            (unless (= (aref connack 3) 0)
              (error "MQTT CONNACK refused, code ~A" (aref connack 3))))
          (setf *mqtt-socket* sock
                *mqtt-stream* stream)
          (format t "~&[MQTT] Connected to ~A:~A~%" *mqtt-host* *mqtt-port*)
          t))
    (error (e)
      (format t "~&[MQTT] Connect failed: ~A~%" e)
      (setf *mqtt-socket* nil *mqtt-stream* nil)
      nil)))

(defun mqtt-ensure-connected ()
  "Reconnect if the persistent connection is gone. Returns T if connected."
  (or (and *mqtt-socket* *mqtt-stream*)
      (mqtt-connect)))

(defun mqtt-publish (topic message)
  "Publish MESSAGE to TOPIC over the persistent MQTT connection.
   Reconnects automatically if the connection has dropped.
   Thread-safe via *mqtt-lock*. Returns T on success, NIL on error."
  (sb-thread:with-mutex (*mqtt-lock*)
    (handler-case
        (progn
          (unless (mqtt-ensure-connected)
            (error "No MQTT connection"))
          (write-sequence (mqtt-publish-packet topic message) *mqtt-stream*)
          (force-output *mqtt-stream*)
          t)
      (error (e)
        (format t "~&[MQTT] Publish failed (~A): ~A — reconnecting...~%" topic e)
        ;; Tear down broken socket so next call reconnects
        (ignore-errors (usocket:socket-close *mqtt-socket*))
        (setf *mqtt-socket* nil *mqtt-stream* nil)
        nil))))

(defun mqtt-timestamp ()
  "Return current time as an ISO-8601 string."
  (local-time:format-timestring nil (local-time:now)
                                :format local-time:+iso-8601-format+))

(defun json-escape (s)
  "Escape double quotes and backslashes in string S for JSON embedding."
  (with-output-to-string (out)
    (loop for ch across (format nil "~A" s)
          do (cond ((char= ch #\") (write-string "\\\"" out))
                   ((char= ch #\\) (write-string "\\\\" out))
                   (t (write-char ch out))))))

(defun mqtt-make-payload (&rest key-value-pairs)
  "Build a JSON object string from alternating string KEY VALUE pairs.
   Values are already formatted as strings (quoted or unquoted as needed)."
  (with-output-to-string (out)
    (write-char #\{ out)
    (loop for (k v) on key-value-pairs by #'cddr
          for first = t then nil
          do (unless first (write-char #\, out))
             (format out "\"~A\":~A" k v))
    (write-char #\} out)))

(defun mqtt-quoted (s)
  "Wrap string S in JSON double quotes."
  (concatenate 'string "\"" (json-escape (format nil "~A" s)) "\""))

(defun mqtt-publish-status (status)
  "Publish to <prefix>/status with STATUS string e.g. online or offline."
  (let ((payload (mqtt-make-payload
                   "status"    (mqtt-quoted status)
                   "timestamp" (mqtt-quoted (mqtt-timestamp)))))
    (mqtt-publish (concatenate 'string *mqtt-topic-prefix* "/status") payload)))

(defun mqtt-publish-poll (asset-count &optional smp)
  "Publish a poll-success summary to <prefix>/poll, including SMP if available."
  (let ((payload (mqtt-make-payload
                   "status"               (mqtt-quoted "ok")
                   "assets"               (format nil "~D" asset-count)
                   "system_marginal_price" (if smp
                                              (format nil "~A" smp)
                                              (mqtt-quoted "unknown"))
                   "consecutive_failures" (format nil "~D" *consecutive-failures*)
                   "timestamp"            (mqtt-quoted (mqtt-timestamp)))))
    (mqtt-publish (concatenate 'string *mqtt-topic-prefix* "/poll") payload)))

(defun mqtt-publish-error (reason details)
  "Publish an error notice to <prefix>/error and increment the failure counter."
  (incf *consecutive-failures*)
  (let ((payload (mqtt-make-payload
                   "reason"               (mqtt-quoted reason)
                   "details"              (mqtt-quoted details)
                   "consecutive_failures" (format nil "~D" *consecutive-failures*)
                   "timestamp"            (mqtt-quoted (mqtt-timestamp)))))
    (mqtt-publish (concatenate 'string *mqtt-topic-prefix* "/error") payload)))

;;; ──────────────────────────────────────────────────────────────────
;;; 5.  Fetch AESO CSD data
;;; ──────────────────────────────────────────────────────────────────

;;; usocket caches NAME-SERVICE-ERROR for the process lifetime after the first
;;; DNS failure. The workaround is to retry with a short sleep — the underlying
;;; C resolver gets a fresh attempt each call and usually succeeds on retry.
(defparameter *dns-retry-attempts* 3
  "Number of times to retry a fetch function on DNS failure before giving up.")

(defparameter *dns-retry-delay* 5
  "Seconds to wait between DNS retry attempts.")

(defun with-dns-retry (fetch-fn label)
  "Call FETCH-FN up to *DNS-RETRY-ATTEMPTS* times, retrying on NAME-SERVICE-ERROR.
   Returns the result of FETCH-FN or NIL if all attempts fail."
  (loop for attempt from 1 to *dns-retry-attempts*
        do (handler-case
               (let ((result (funcall fetch-fn)))
                 (return result))
             (usocket:ns-error (e)
               (format t "~&[WARN] ~A DNS failure (attempt ~A/~A): ~A~%"
                       label attempt *dns-retry-attempts* e)
               (if (< attempt *dns-retry-attempts*)
                   (progn
                     (format t "~&[INFO] Retrying in ~As...~%" *dns-retry-delay*)
                     (sleep *dns-retry-delay*))
                   (progn
                     (format t "~&[ERROR] ~A DNS failed after ~A attempts. Will retry next poll.~%"
                             label *dns-retry-attempts*)
                     (mqtt-publish-error
                       (format nil "~A_dns_failure" label)
                       (format nil "~A" e))
                     (return nil)))))
        finally (return nil)))

;;; OpenSSL 3.x strictly requires a TLS close_notify alert on shutdown.
;;; Azure API Management drops the TCP connection without sending one, which
;;; causes error:0A000126 (SSL-ERROR-SSL / unexpected eof while reading).
;;;
;;; Fix: define SSL_CTX_set_options via CFFI (cl+ssl does not export it) and
;;; call it on a freshly created context, then activate that context with
;;; cl+ssl:with-global-context for every outbound request.
;;; SSL_OP_IGNORE_UNEXPECTED_EOF (#x00000080, OpenSSL 3+ only) tells OpenSSL
;;; to treat a missing close_notify as a normal EOF rather than a fatal error.

(cffi:defcfun ("SSL_CTX_set_options" %ssl-ctx-set-options) :long
  (ctx  :pointer)
  (options :long))

(defconstant +ssl-op-ignore-unexpected-eof+ #x00000080
  "OpenSSL 3 flag: treat missing close_notify as graceful shutdown.")

(defun make-azure-tolerant-ssl-ctx ()
  "Create a cl+ssl SSL_CTX with SSL_OP_IGNORE_UNEXPECTED_EOF set."
  (let ((ctx (cl+ssl:make-context)))
    (%ssl-ctx-set-options ctx +ssl-op-ignore-unexpected-eof+)
    ctx))

(defun fetch-csd ()
  "Call the AESO CSD v2 API and return a parsed JSON hash-table, or NIL on error."
  (handler-case
      (cl+ssl:with-global-context ((make-azure-tolerant-ssl-ctx) :auto-free-p t)
        (let* ((response (dex:get *aeso-csd-url*
                                  :headers `(("API-KEY" . ,*aeso-api-key*)
                                             ("Accept"                    . "application/json"))
                                  :use-connection-pool nil))
               (json (yason:parse response :object-as :hash-table)))
          (when *debug-json*
            (format t "~&[DEBUG] Raw JSON keys at top level: ~A~%"
                    (when (hash-table-p json)
                      (loop for k being the hash-keys of json collect k)))
            (let ((ret (or (gethash "return" json) json)))
              (format t "~&[DEBUG] Keys under 'return': ~A~%"
                      (when (hash-table-p ret)
                        (loop for k being the hash-keys of ret collect k)))
              (let ((assets (or (gethash "asset_list" ret)
                                (gethash "assets" ret)
                                (gethash "data"   ret))))
                (when (and assets (listp assets) (plusp (length assets)))
                  (format t "~&[DEBUG] First asset keys: ~A~%"
                          (when (hash-table-p (first assets))
                            (loop for k being the hash-keys of (first assets) collect k)))
                  (format t "~&[DEBUG] First asset values: ~A~%"
                          (when (hash-table-p (first assets))
                            (loop for k being the hash-keys of (first assets)
                                  collect (cons k (gethash k (first assets))))))))))
          json))
    ;; HTTP-level errors – give specific guidance per status code
    (dex:http-request-failed (e)
      (let ((status (dex:response-status e))
            (body   (dex:response-body e)))
        (format t "~&[ERROR] AESO HTTP ~A~%" status)
        (cond
          ((= status 401)
           (format t "~&        -> 401 Unauthorized: API key missing or invalid.~%~
                              Check AESO_API_KEY and that your subscription is active~%~
                              at https://developer-apim.aeso.ca/~%"))
          ((= status 403)
           (format t "~&        -> 403 Forbidden: key valid but no access to this product.~%~
                              Ensure your APIM subscription includes the CSD v2 API.~%"))
          ((= status 404)
           (format t "~&        -> 404 Not Found: endpoint may have changed.~%~
                              Current URL: ~A~%~
                              Check https://developer-apim.aeso.ca/ for the current path.~%"
                   *aeso-csd-url*))
          (t (format t "~&        Response body: ~A~%" body))))
      nil)
    ;; DNS resolution failure
    (usocket:ns-error (e)
      (format t "~&[ERROR] DNS failure resolving ~A~%" *aeso-csd-url*)
      (format t "~&        ~A~%" e)
      (format t "~&        Checklist:~%~
                          1. Outbound internet access?  Try: curl -I https://apimgw.aeso.ca~%~
                          2. Firewall/proxy blocking HTTPS?~%~
                             Try: curl --proxy http://PROXY:PORT -I https://apimgw.aeso.ca~%~
                          3. DNS working?  Try: host apimgw.aeso.ca~%~
                          If behind a proxy set: export HTTPS_PROXY=http://host:port~%")
      nil)
    ;; TCP-level: connection refused / timeout
    (usocket:socket-error (e)
      (format t "~&[ERROR] Socket error connecting to AESO API: ~A~%" e)
      (format t "~&        Host resolved but connection was refused or timed out.~%")
      nil)
    (error (e)
      (format t "~&[ERROR] fetch-csd unexpected error (~A): ~A~%"
              (type-of e) e)
      nil)))

;;; ──────────────────────────────────────────────────────────────────
;;; 5b. Fetch AESO SMP / Pool Price
;;; ──────────────────────────────────────────────────────────────────

(defun fetch-smp ()
  "Call the AESO Pool Price API and return parsed JSON, or NIL on error."
  (handler-case
      (cl+ssl:with-global-context ((make-azure-tolerant-ssl-ctx) :auto-free-p t)
        (let* ((response (dex:get *aeso-smp-url*
                                  :headers `(("API-KEY" . ,*aeso-api-key*)
                                             ("Accept"  . "application/json"))
                                  :use-connection-pool nil))
               (json (yason:parse response :object-as :hash-table)))
          json))
    (dex:http-request-failed (e)
      (format t "~&[ERROR] AESO SMP HTTP ~A: ~A~%" (dex:response-status e) (dex:response-body e))
      nil)
    (usocket:ns-error (e)
      (format t "~&[ERROR] SMP DNS failure: ~A~%" e) nil)
    (usocket:socket-error (e)
      (format t "~&[ERROR] SMP socket error: ~A~%" e) nil)
    (error (e)
      (format t "~&[ERROR] fetch-smp unexpected (~A): ~A~%" (type-of e) e) nil)))

(defun smp->line-protocol (json ts-ns)
  "Convert SMP API JSON to InfluxDB line-protocol strings.
   Measurement: aeso_smp
   Fields: system_marginal_price ($/MWh), volume (MW)"
  (handler-case
      (let* ((ret     (gethash "return" json))
             (reports (and ret (gethash "System Marginal Price Report" ret)))
             (entry   (and reports (listp reports) (first reports))))
        (unless entry
          (format t "~&[WARN] SMP: no 'System Marginal Price Report' in response~%")
          (return-from smp->line-protocol nil))
        (let ((flds '()))
          (flet ((add (lp-key json-key)
                   (let ((n (safe-number (gethash json-key entry))))
                     (when n (push (format nil "~A=~A" lp-key n) flds)))))
            (add "system_marginal_price" "system_marginal_price")
            (add "volume"                "volume"))
          (when flds
            (list (format nil "aeso_smp ~{~A~^,~} ~D"
                          (nreverse flds) ts-ns)))))
    (error (e)
      (format t "~&[ERROR] smp->line-protocol: ~A~%" e)
      nil)))

;;; ──────────────────────────────────────────────────────────────────
;;; 6.  Transform JSON → InfluxDB line-protocol strings
;;;
;;;  The AESO CSD assets endpoint returns:
;;;
;;;  {
;;;    "return": {
;;;      "timestamp": "2026-05-16T03:00:00-06:00",
;;;      "assets": [
;;;        { "assetId":   "ABCD",
;;;          "assetName": "Some Plant",
;;;          "fuelType":  "GAS",
;;;          "mcr":  400.0,
;;;          "tng":  350.0,
;;;          "dcr":   10.0 },
;;;        ...
;;;      ]
;;;    }
;;;  }
;;;
;;;  We write one measurement:
;;;    csd_asset – one row per asset, tagged with asset_id, asset_name, fuel_type
;;; ──────────────────────────────────────────────────────────────────

(defun json->line-protocol (json ts-ns)
  "Convert CSD assets JSON to a list of InfluxDB line-protocol strings.
   Measurement: csd_asset
   Tags:   asset, fuel_type, sub_fuel_type
   Fields: maximum_capability, net_generation, dispatched_contingency_reserve"
  (let* ((ret        (gethash "return" json))
         (asset-list (and ret (gethash "asset_list" ret)))
         (lines      '()))
    (unless (and asset-list (listp asset-list))
      (format t "~&[WARN] 'asset_list' missing or not a list in JSON return block.~%")
      (return-from json->line-protocol nil))
    (dolist (entry asset-list)
      (when (hash-table-p entry)
        (let* ((asset    (escape-lp-string
                           (or (gethash "asset"        entry) "UNKNOWN")))
               (fuel     (escape-lp-string
                           (or (gethash "fuel_type"    entry) "UNKNOWN")))
               (subfuel  (escape-lp-string
                           (let ((s (gethash "sub_fuel_type" entry)))
                             (if (or (null s) (equal s "")) "NONE" s))))
               (tags  (format nil "asset=~A,fuel_type=~A,sub_fuel_type=~A"
                              asset fuel subfuel))
               (flds  '()))
          (flet ((add (lp-key json-key)
                   (let ((n (safe-number (gethash json-key entry))))
                     (when n (push (format nil "~A=~A" lp-key n) flds)))))
            (add "maximum_capability"             "maximum_capability")
            (add "net_generation"                 "net_generation")
            (add "dispatched_contingency_reserve" "dispatched_contingency_reserve"))
          (when flds
            (push (format nil "csd_asset,~A ~{~A~^,~} ~D"
                          tags (nreverse flds) ts-ns)
                  lines)))))
    (nreverse lines)))

;;; ──────────────────────────────────────────────────────────────────
;;; 7.  Write to InfluxDB (line-protocol HTTP write API)
;;; ──────────────────────────────────────────────────────────────────

(defun url-encode (s)
  "Percent-encode string S for use in a URL query parameter."
  (with-output-to-string (out)
    (loop for ch across s
          do (if (or (char<= #\a ch #\z)
                     (char<= #\A ch #\Z)
                     (char<= #\0 ch #\9)
                     (member ch '(#\- #\_ #\. #\~)))
                 (write-char ch out)
                 (format out "%~2,'0X" (char-code ch))))))

(defun influx-write-url ()
  "Build the InfluxDB v2 write endpoint URL."
  (format nil "~A/api/v2/write?org=~A&bucket=~A&precision=ns"
          *influx-url*
          (url-encode *influx-org*)
          (url-encode *influx-bucket*)))

(defun write-to-influx (lines)
  "POST LINE-PROTOCOL LINES (a list of strings) to InfluxDB.
   Returns T on success, NIL on any error."
  (when lines
    (let ((body (format nil "~{~A~%~}" lines)))
      (handler-case
          (progn
            (dex:post (influx-write-url)
                      :headers `(("Authorization" . ,(format nil "Token ~A" *influx-token*))
                                 ("Content-Type"  . "text/plain; charset=utf-8"))
                      :content body)
            (format t "~&[OK] Wrote ~A line(s) to InfluxDB.~%" (length lines))
            t)
        (dex:http-request-failed (e)
          (format t "~&[ERROR] InfluxDB write HTTP ~A: ~A~%"
                  (dex:response-status e) (dex:response-body e))
          nil)
        (error (e)
          (format t "~&[ERROR] write-to-influx: ~A~%" e)
          nil)))))

;;; ──────────────────────────────────────────────────────────────────
;;; 8.  Single fetch-and-write cycle
;;; ──────────────────────────────────────────────────────────────────

(defun run-once ()
  "Fetch AESO CSD + SMP data, push to InfluxDB, publish MQTT notifications."
  (format t "~&[~A] Fetching AESO CSD + SMP...~%"
          (local-time:format-timestring nil (local-time:now)
                                        :format local-time:+iso-8601-format+))
  (let ((ts-ns (now-unix-ns))
        (all-lines '())
        (last-smp nil))   ; holds the SMP value for MQTT

    ;; ── CSD asset generation ────────────────────────────────────────
    (let ((json (with-dns-retry #'fetch-csd "csd")))
      (if json
          (let ((lines (json->line-protocol json ts-ns)))
            (if lines
                (setf all-lines (append all-lines lines))
                (progn
                  (format t "~&[WARN] No CSD line-protocol entries – check JSON structure.~%")
                  (mqtt-publish-error "no_line_protocol" "CSD JSON produced no entries"))))
          (progn
            (format t "~&[WARN] No data from AESO CSD API.~%")
            (mqtt-publish-error "csd_fetch_failed" "AESO CSD API returned no data"))))

    ;; ── Pool price / SMP ────────────────────────────────────────────
    (let ((json (with-dns-retry #'fetch-smp "smp")))
      (if json
          (let ((lines (smp->line-protocol json ts-ns)))
            (if lines
                (progn
                  ;; Extract SMP value for MQTT from the JSON directly
                  (let* ((ret     (gethash "return" json))
                         (reports (and ret (gethash "System Marginal Price Report" ret)))
                         (entry   (and reports (listp reports) (first reports))))
                    (when entry
                      (setf last-smp (gethash "system_marginal_price" entry))))
                  (format t "~&[INFO] SMP: $~A/MWh~%" (or last-smp "?"))
                  (setf all-lines (append all-lines lines)))
                (format t "~&[WARN] No SMP line-protocol entries.~%")))
          (format t "~&[WARN] No data from AESO SMP API.~%")))

    ;; ── Write everything to InfluxDB in one batch ───────────────────
    (if all-lines
        (progn
          (format t "~&[INFO] Writing ~A total line(s) to InfluxDB.~%" (length all-lines))
          (let ((write-ok (write-to-influx all-lines)))
            (if write-ok
                (progn
                  (setf *consecutive-failures* 0)
                  (mqtt-publish-poll (length all-lines) last-smp))
                (progn
                  (mqtt-publish-error "influx_write_failed" "InfluxDB write error")
                  (format t "~&[MQTT] Error notice published.~%")))))
        (format t "~&[WARN] No lines to write.~%"))))

;;; ──────────────────────────────────────────────────────────────────
;;; 9.  Heartbeat – publishes uptime + unix time to MQTT every second
;;; ──────────────────────────────────────────────────────────────────

(defparameter *start-time* nil
  "Unix timestamp (seconds) when the collector started. Set in main.")

(defun uptime-seconds ()
  "Return integer seconds since *start-time* was set."
  (- (local-time:timestamp-to-unix (local-time:now)) *start-time*))

(defun format-uptime (secs)
  "Format SECS as Dd HH:MM:SS string."
  (let* ((d  (floor secs 86400))
         (r  (mod secs 86400))
         (h  (floor r 3600))
         (r2 (mod r 3600))
         (m  (floor r2 60))
         (s  (mod r2 60)))
    (if (> d 0)
        (format nil "~Dd ~2,'0D:~2,'0D:~2,'0D" d h m s)
        (format nil "~2,'0D:~2,'0D:~2,'0D" h m s))))

(defun heartbeat-loop ()
  "Publish uptime and unix timestamp to MQTT every second. Runs forever."
  (loop
    (let* ((now-unix  (local-time:timestamp-to-unix (local-time:now)))
           (uptime-s  (- now-unix *start-time*))
           (payload   (mqtt-make-payload
                        "unix_time"  (format nil "~D" now-unix)
                        "uptime_sec" (format nil "~D" uptime-s)
                        "uptime"     (mqtt-quoted (format-uptime uptime-s))
                        "failures"   (format nil "~D" *consecutive-failures*)
                        "timestamp"  (mqtt-quoted (mqtt-timestamp)))))
      (mqtt-publish (concatenate 'string *mqtt-topic-prefix* "/heartbeat") payload))
    (sleep 1)))

;;; ──────────────────────────────────────────────────────────────────
;;; 10. Entry point – poll or run once
;;; ──────────────────────────────────────────────────────────────────

;;; Exit after this many consecutive poll failures — lets the shell restart loop take over
(defparameter *max-consecutive-failures* 10
  "Exit the process after this many consecutive failures so the shell restart loop can recover.")

(defun main ()
  (format t "~&AESO CSD → InfluxDB/Grafana collector~%")
  (format t "~&  AESO endpoint : ~A~%" *aeso-csd-url*)
  (format t "~&  InfluxDB      : ~A  bucket=~A~%" *influx-url* *influx-bucket*)
  (format t "~&  MQTT broker   : ~A:~A  prefix=~A~%~%"
          *mqtt-host* *mqtt-port* *mqtt-topic-prefix*)

  ;; Record start time
  (setf *start-time* (local-time:timestamp-to-unix (local-time:now)))

  ;; Establish persistent MQTT connection
  (format t "~&[MQTT] Connecting to broker...~%")
  (mqtt-connect)

  ;; Publish startup notice
  (format t "~&[MQTT] Publishing startup notice...~%")
  (mqtt-publish-status "online")

  ;; Launch heartbeat in a background thread (SBCL threads)
  (format t "~&[MQTT] Starting heartbeat thread (1s interval)...~%")
  (sb-thread:make-thread #'heartbeat-loop :name "mqtt-heartbeat")

  (if *poll-interval-seconds*
      (progn
        (loop
          (run-once)
          ;; Exit if too many consecutive failures — shell restart loop will recover
          (when (>= *consecutive-failures* *max-consecutive-failures*)
            (format t "~&[ERROR] ~A consecutive failures — exiting for restart.~%"
                    *consecutive-failures*)
            (mqtt-publish-status "offline")
            (sb-ext:exit :code 1))
          (format t "~&[INFO] Sleeping ~As...~%" *poll-interval-seconds*)
          (sleep *poll-interval-seconds*))
        (mqtt-publish-status "offline"))
      (progn
        (run-once)
        (mqtt-publish-status "offline"))))

(main)
