(ns athena.lambda
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.trace :as t]
            [cheshire.core :as json]
            [puget.printer :as puget]
            [orca.core :as orca]
            [athena.tools :as tools]
            [clojure.set :as set]
            [clojure.string :as str]
            [orca.core :as orc]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.lambda :as lambda]
            [taoensso.timbre :as timbre])
  (:import (org.apache.orc OrcFile TypeDescription TypeDescription$Category)
           (java.time LocalDate Instant ZonedDateTime ZoneId)
           (java.time.temporal ChronoUnit)
           (java.time.format DateTimeFormatter)
           (java.io File)
           (com.amazonaws.services.lambda.runtime RequestStreamHandler)
           (org.apache.commons.io FilenameUtils)))

(timbre/refer-timbre)

(timbre/set-level! :warn)

(defn cprint [x]
  (puget/pprint x {:print-color true}))

(defn hour-floor [^Instant instant]
  (.truncatedTo instant (ChronoUnit/HOURS)))

(defn basename [path]
  (last (str/split path #"/")))

(def ^:dynamic *aws-auth* {:profile "production"})

(defn download-object
  "Gets the s3 object identified by object-summary and copies the results into the returned tmpfile"
  [obj-summary]
  (let [{object-key :key :keys [object-content] :as obj} (s3/get-object *aws-auth* obj-summary)
        local-file (io/file "/tmp/" (basename object-key))]
    (info "downloading" local-file)
    (io/copy object-content local-file)
    local-file))

(def partition-format (DateTimeFormatter/ofPattern "YYYY-MM-dd-HH"))

(defn keywordize [variable-name]
  (-> variable-name
      (str/replace #"_" "-")
      str/lower-case
      keyword))

(defmacro with-delete
  "Deletes any files in bindings"
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-delete ~(subvec bindings 2) ~@body)
                                (finally
                                  (.delete ~(bindings 0)))))
    :else (throw (IllegalArgumentException.
                  "only Symbols may be bound"))))

(comment
  {:destination-s3-bucket nil
   :destination-s3-prefix "/"
   :destination-s3-region "us-east-1"
   :source-s3-format      "csv|tsv|json"
   :partition-by          ":foo"
   :partition-by-format   "timestamp"
   :partition-key-format  "instant"
   :partition-key-name    "dt"
   :orc-schema            "struct<check_in:date,nights:tinyint>"})


(defn encode-files
  "Encodes a collection of files as Apache ORC."
  [output-path schema input-paths]
  (->> input-paths
       (map (comp tools/input-reader io/file))
       (transduce (mapcat tools/row-parser) (orca/file-encoder output-path schema 1024 {:overwrite? true}))))

(defn output-file
  "Constructs a path on /tmp for encoding the ORC file"
  ([input] (output-file "/tmp" input))
  ([parent input]
   (let [input-file  (io/file input)
         filename    (.getName input-file)
         filename    (str/replace filename #"\.(gz|bzip2|gzip|snappy|sz)$" "")]
     (io/file parent (str filename ".orc")))))

(defn process-file
  "Processes file into one or more output ORC files then uploads to S3."
  [input {:keys [destination-s3-bucket destination-s3-prefix orc-schema] :as env-map}]
  (with-delete [output-file (output-file input)]
    (println output-file)))

(defmacro deflambdafn
  "Create a named class that can be invoked as a AWS Lambda function.  Taken from https://github.com/uswitch/lambada."
  [name args & body]
  (assert (= (count args) 3) "lambda function must have exactly three args")
  (let [prefix (gensym)
        handleRequestMethod (symbol (str prefix "handleRequest"))]
    `(do
       (gen-class
        :name ~name
        :prefix ~prefix
        :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
       (defn ~handleRequestMethod
         ~(into ['this] args)
         ~@body))))

(defn json->objects [in]
  (with-open [rdr (io/reader in)]
    (->> (json/parse-stream rdr keyword)
         :Records
         (map :s3)
         (map #(hash-map :bucket-name (get-in % [:bucket :name]) :key (get-in % [:object :key]))))))

(defn env
  "Retun the envionment as a keyword map"
  ([] (env (System/getenv)))
  ([env-variables]
   (->> env-variables
        (into {})
        (reduce-kv #(assoc %1 (keywordize %2) %3) {}))))

(deflambdafn OrcS3EventNotificationEncoder
  [in out ctx]
  (let [env-map       (env)
        input-objects (json->objects in)]
    (with-open [output (io/writer out)]
      (doseq [file (map download-object input-objects)]
        (process-file file env-map)
        (.delete file))
      (json/generate-stream input-objects output))))

;; (defn -main [& args]
;;   (->> (encode-files))
;;   (doseq [file args]
;;     ))
