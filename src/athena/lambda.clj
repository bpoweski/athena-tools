(ns athena.lambda
  (:require [amazonica.aws.s3 :as s3]
            [athena.tools :as tools]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [orca.core :as orca]
            [taoensso.timbre :as timbre])
  (:import com.amazonaws.services.lambda.runtime.RequestStreamHandler
           com.amazonaws.services.s3.AmazonS3URI
           (org.xerial.snappy SnappyFramedOutputStream)
           [java.time Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter))

(timbre/refer-timbre)

(timbre/set-level! :warn)

(defn basename [path]
  (last (str/split path #"/")))

(defn download-object
  "Gets the s3 object identified by object-summary and copies the results into the returned tmpfile"
  [obj-summary]
  (let [{object-key :key :keys [object-content] :as obj} (s3/get-object obj-summary)
        local-file (io/file "/tmp/" (basename object-key))]
    (info "downloading" local-file)
    (io/copy object-content local-file)
    local-file))

(defn from-millis [x]
  (ZonedDateTime/ofInstant (Instant/ofEpochMilli x) (ZoneId/of "UTC")))

(defn from-epoch [x]
  (ZonedDateTime/ofInstant (Instant/ofEpochSecond x) (ZoneId/of "UTC")))

(defn format-instant [x pattern]
  (.format x (DateTimeFormatter/ofPattern pattern)))

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

(defn output-file
  "Constructs a path on /tmp for encoding the ORC file"
  [parent input]
  (let [input-file  (io/file input)
        filename    (.getName input-file)
        filename    (str/replace filename #"\.(gz|bzip2|gzip|snappy|sz)$" "")]
    (io/file parent (str filename ".orc"))))

(defn md5 [s]
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes s))]
    (format "%032x" (BigInteger. 1 raw))))

(defn partition-fn [{:keys [partition-key partition-by]}]
  (when-not (or (str/blank? partition-by) (str/blank? partition-key))
    (let [form (read-string partition-by)]
      (cond
        (symbol? form)
        (keyword form)

        (instance? clojure.lang.IFn form)
        form

        (and (list? form) (= 'fn (first form)))
        (eval form)))))

(defn spool-by
  "Spools elements to disk by f.  Serializes elements using pr-str & read-string.  Returns a map of (f x) -> path"
  [f]
  (let [file-partitions (volatile! {})]
    (fn
      ([])
      ([result]
       (let [partitions @file-partitions]
         (doseq [[k [_ ^java.io.BufferedWriter writer]] partitions]
           (.close writer))
         (zipmap (keys partitions) (map (comp io/file first) (vals partitions)))))
      ([result x]
       (let [k      (f x)
             writer (if-let [[_ ^java.io.BufferedWriter writer] (get @file-partitions k)]
                      writer
                      (let [path   (orca/tmp-path)
                            writer (io/writer (SnappyFramedOutputStream. (io/output-stream path)))]
                        (info "spooling rows of" k "to" path)
                        (vswap! file-partitions assoc k [path writer])
                        writer))]
         (.write writer (pr-str x))
         (.newLine writer)
         result)))))

(defn spool-reader
  "Returns an clojure.lang.IReduceInit for path of a spooled file."
  [file]
  (-> file
      tools/input-reader
      tools/reducible-lines))

(defn process-file
  "Processes file into one or more output ORC files then uploads to S3."
  [input {:keys [destination-s3-bucket destination-s3-prefix partition-key orc-schema] :as env-map}]
  {:pre [(string? destination-s3-bucket)]}
  (if-let [f (partition-fn env-map)]
    (doseq [[partition spool-file] (transduce (mapcat tools/row-parser) (spool-by f) [(tools/input-reader (io/file input))])
            :let [partition-prefix (str partition-key "=" partition)]]
      (with-delete [output     (output-file (str "/tmp/" partition-prefix) input)
                    spool-file spool-file]
        (transduce (map edn/read-string) (orca/file-encoder output orc-schema 1024 {:overwrite? true}) (spool-reader spool-file))
        (s3/put-object {:bucket-name destination-s3-bucket
                        :key (str destination-s3-prefix partition-prefix "/" (.getName output))
                        :file output
                        :metadata {:user-metadata {:orc-schema-md5 (md5 orc-schema)}}})))
    (with-delete [output (output-file "/tmp" input)]
      (tools/encode-files output orc-schema [input])
      (s3/put-object {:bucket-name destination-s3-bucket
                      :key (str destination-s3-prefix (.getName output))
                      :file output
                      :metadata {:user-metadata {:orc-schema-md5 (md5 orc-schema)}}}))))

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
        (reduce-kv (fn [ret k v] (if (str/blank? v) ret (assoc ret (keywordize k) v))) {}))))

(deflambdafn athena.lambda.OrcS3EventNotificationEncoder
  [in out ctx]
  (let [env-map       (env)
        input-objects (json->objects in)]
    (with-open [lambda-output (io/writer out)]
      (doseq [file (map download-object input-objects)]
        (process-file file env-map)
        (.delete file))
      (json/generate-stream input-objects lambda-output))))
