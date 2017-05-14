(ns athena.tools
  (:require [cheshire.core :as json]
            [puget.printer :as puget]
            [orca.core :as orca]
            [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [puget.printer :as puget]
            [clojure.tools.trace :as t]
            [orca.core :as orc])
  (:import (org.xerial.snappy SnappyFramedInputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream)
           (org.apache.orc TypeDescription)
           (java.nio.file StandardOpenOption Files))
  (:gen-class))


(defn cprint [x]
  (puget/pprint x {:print-color true}))

(def cli-options
  [["-o" "--output PATH" "Output file"]
   [nil "--encode" "Encode files as ORC"
    :default false]
   [nil "--discover" "Discover the ORC schema"
    :default false]
   [nil "--override-struct KEY VALUE"
    :assoc-fn (fn [m k v]
                (let [[member value] (str/split v #":" 2)]
                  (assoc-in m [k (keyword member)] (orca/schema->typedef (TypeDescription/fromString value)))))]
   [nil "--pretty" "Pretty print the discovered typedef."
    :default false]
   [nil "--coerce-decimal-strings" "Attempt to coerce a decimal from a string."
    :id :coerce-decimal-strings? :default false]
   [nil "--min-decimal-precision PRECISION" "Sets a minimum precision for decimals via schema discovery."
    :parse-fn #(Integer/parseUnsignedInt %)]
   [nil "--min-decimal-scale SCALE" "Sets a minimum scale for decimals via schema discovery."
    :parse-fn #(Integer/parseUnsignedInt %)]
   [nil "--coerce-date-strings" "Attempt to coerce a date from a string."
    :id :coerce-date-strings? :default false]
   [nil "--coerce-timestamp-strings" "Attempt to coerce a timestamp from a string."
    :id :coerce-timestamp-strings? :default false]
   [nil "--create-table" "Show Athena CREATE TABLE"
    :id :create-table?]
   [nil "--table-name TABLE" "Athena table name"
    :default "table"]
   [nil "--s3-location LOCATION" "S3 Location"
    :default "bucket-name"]
   ["-s" "--schema SCHEMA" "ORC schema"]
   ["-h" "--help"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn usage [options-summary]
  (->> ["This is my program. There are many like it, but this one is mine."
        ""
        "Usage: java -jar ahtena-tools.jar [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn prefix-equal? [x y]
  (every? true? (map = x y)))

(defn compression-encoding [in]
  (let [file (java.io.RandomAccessFile. (io/file in) "r")]
    (condp prefix-equal? (repeatedly 10 #(.readUnsignedByte file))
      [0x42 0x5a]                                         :bzip2
      [0x1f 0x8b]                                         :gzip
      [0xff 0x06 0x00 0x00 0x73 0x4e 0x61 0x50 0x70 0x59] :snappy-framed
      nil)))

(defn input-reader [file]
  (io/reader
   (condp = (compression-encoding file)
     :snappy-framed (SnappyFramedInputStream. (io/input-stream file))
     :gzip          (GzipCompressorInputStream. (io/input-stream file) true)
     :bzip2         (BZip2CompressorInputStream. (io/input-stream file) true)
     file)))

(defn parse-line [line]
  (json/parse-string line keyword))

(defn path->typedef [path options]
  (println "discovering schema for" path)
  (with-open [rdr (input-reader (io/file path))]
    (try
      (orca/rows->typedef
       (->> rdr
            line-seq
            (random-sample 0.05)
            (map parse-line))
       options)
      (catch clojure.lang.ExceptionInfo ex
        (cprint (ex-data ex))
        (throw ex)))))

(defn discover-typedef [{:keys [arguments options]}]
  {:pre (seq arguments)}
  (->> arguments
       (map #(path->typedef % options))
       (reduce orca/merge-typedef)))

(defn escape-schema [field]
  (str/replace field #"(<|^|,)(_[^:]*)" "$1`$2`"))

(defn escape-field [field]
  (if (or (str/starts-with? field "_") (re-find #"[\?]" field))
    (str "`" field"`")
    field))

(defn create-table-sql
  "Generate the CREATE TABLE statement for a given schema"
  [table-name schema-str location]
  (let [schema             (TypeDescription/fromString schema-str)
        location-statement (str/join \newline ["STORED AS ORC" (format "LOCATION '%s'" location)])
        column-statements  (map #(str/join "  " %) (map vector (map escape-field (.getFieldNames schema)) (map (comp escape-schema str) (.getChildren schema))))]
    (str/join
     \newline
     (-> [(format "CREATE EXTERNAL TABLE %s (" table-name)]
         (into (map #(str % ",") (butlast column-statements)))
         (conj (last column-statements) ")"
               "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"
               "WITH SERDEPROPERTIES ('serialization.format' = '1')"
               (format "LOCATION '%s'" location))))))

(defn discover-schema [{:keys [arguments options] :as opts}]
  (let [typedef (discover-typedef opts)]
    (cond
      (:pretty options)        (cprint typedef)
      (:create-table? options) (println (create-table-sql (:table-name options) (str (orca/typedef->schema typedef)) (:s3-location options)))
      :else                    (-> typedef
                                   orca/typedef->schema
                                   str
                                   println))))

(defn schema [{:keys [options arguments] :as opts}]
  (if-let [schema (:schema options)]
    schema
    (when (:discover options)
      (-> opts
          discover-typedef
          orca/typedef->schema
          str))))

;; useful technique from https://tech.grammarly.com/blog/building-etl-pipelines-with-clojure
(defn reducible-lines
  "Like line-seq but takes care of the dangling IO issue"
  [^java.io.BufferedReader rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if-let [line (.readLine rdr)]
            (recur (f state line))
            state))
        (finally (.close rdr))))))

(defn row-parser
  "Creates an eduction of rdr that parsers each line."
  [rdr]
  (eduction (map parse-line) (reducible-lines rdr)))

(defn file-exists? [path]
  (let [file (io/file path)
        exists? (.exists file)]
    (when-not exists?
      (println "WARN: file" path "does not exist"))
    exists?))

(defn encode-files
  "Encodes ORC per CLI options"
  [{:keys [arguments options] :as opts}]
  {:pre [(string? (:output options))]}
  (let [schema (schema opts)]
    (assert schema "a schema is required")
    (->> arguments
         (map input-reader)
         (transduce (mapcat row-parser) (orca/file-encoder (:output options) schema 1024 {:overwrite? true})))))

(defn -main [& args]
  (let [{:keys [options arguments errors summary] :as opts} (cli/parse-opts args cli-options)]
    (cond
      (:help options)     (println (usage summary))
      (:encode options)   (encode-files opts)
      (:discover options) (discover-schema opts)
      :else               (println (usage summary)))))
