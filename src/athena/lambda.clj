(ns athena.lambda
  (:require [amazonica.aws.s3 :as s3]
            [athena.tools :as tools]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [orca.core :as orca]
            [taoensso.timbre :as timbre]
            [taoensso.nippy :as nippy])
  (:import com.amazonaws.services.lambda.runtime.RequestStreamHandler
           com.amazonaws.services.s3.AmazonS3URI
           (org.xerial.snappy SnappyFramedOutputStream)
           [java.time Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter))

(timbre/refer-timbre)

(if-let [level (System/getenv "LOG_LEVEL")]
  (timbre/set-level! (keyword level))
  (timbre/set-level! :warn))

(defn basename [path]
  (last (str/split path #"/")))

(defn download-object
  "Gets the s3 object identified by object-summary and copies the results into the returned tmpfile"
  [obj-summary]
  (let [{object-key :key :as obj} (s3/get-object obj-summary)
        local-file (io/file "/tmp/" (basename object-key))]
    (info "downloading" local-file)
    (with-open [is (:object-content obj)]
      (io/copy is local-file))
    local-file))

(defn from-millis [x]
  (ZonedDateTime/ofInstant (Instant/ofEpochMilli x) (ZoneId/of "UTC")))

(defn from-epoch [x]
  (ZonedDateTime/ofInstant (Instant/ofEpochSecond x) (ZoneId/of "UTC")))

(defn format-instant [^java.time.ZonedDateTime x ^String pattern]
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
                                  (let [file# ~(bindings 0)
                                        deleted?# (.delete file#)]
                                    (debug "deleting" file# deleted?#)))))
    :else (throw (IllegalArgumentException.
                  "only Symbols may be bound"))))

(defn ^java.io.File output-file
  "Constructs a path on /tmp for encoding the ORC file"
  [parent input]
  (let [input-file  (io/file input)
        filename    (.getName input-file)
        filename    (str/replace filename #"\.(gz|bzip2|gzip|snappy|sz)$" "")]
    (io/file parent (str filename ".orc"))))

(defn md5 [s]
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes (str s)))]
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
         (doseq [[k [_ ^java.io.DataOutputStream data-output]] partitions]
           (.close data-output))
         (zipmap (keys partitions) (map (comp io/file first) (vals partitions)))))
      ([result x]
       (let [k                               (f x)
             ^java.io.DataOutput data-output (if-let [[_ writer] (get @file-partitions k)]
                                               writer
                                               (let [path        (orca/tmp-path)
                                                     data-output (java.io.DataOutputStream. (SnappyFramedOutputStream. (io/output-stream path)))]
                                                 (info "spooling rows of" k "to" path)
                                                 (vswap! file-partitions assoc k [path data-output])
                                                 data-output))]
         (nippy/freeze-to-out! data-output x)
         result)))))

(defn spool-reader
  "Returns an clojure.lang.IReduceInit for path of a spooled file."
  [^java.io.File file]
  (let [input-stream (java.io.DataInputStream. (tools/compressed-input-stream file))]
    (reify clojure.lang.IReduceInit
      (reduce [this f init]
        (try
          (loop [state init]
            (if-let [x (try
                         (nippy/thaw-from-in! input-stream)
                         (catch java.io.EOFException ex
                           (debug "done reading" (.getPath file))))]
              (recur (f state x))
              state))
          (finally (.close input-stream)))))))

(defn process-file
  "Processes file into one or more output ORC files then uploads to S3."
  [input {:keys [destination-s3-bucket destination-s3-prefix partition-key orc-schema] :as env-map}]
  {:pre [(string? destination-s3-bucket)]}
  (if-let [f (partition-fn env-map)]
    (with-open [input-stream (tools/file-reader input)]
      (doseq [[partition spool-file] (transduce (mapcat tools/row-parser) (spool-by f) [input-stream])
              :let [partition-prefix (str partition-key "=" partition)]]
        (with-delete [output     (output-file (str "/tmp/" partition-prefix) input)
                      spool-file spool-file]
          (transduce (map identity) (orca/file-encoder output orc-schema 1024 {:overwrite? true}) (spool-reader spool-file))
          (s3/put-object {:bucket-name destination-s3-bucket
                          :key (str destination-s3-prefix partition-prefix "/" (.getName output))
                          :file output
                          :metadata {:user-metadata {:orc-schema-md5 (md5 orc-schema)}}}))))
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

(defn orc-file? [^java.io.File file]
  (or (str/ends-with? (.getName file) ".orc.crc")
      (str/ends-with? (.getName file) ".orc")))

(defn clean-tmp []
  (doseq [file (filter orc-file? (file-seq (io/file "/tmp")))]
    (debug "cleaning up ORC file" file)
    (.delete file)))

(deflambdafn athena.lambda.OrcS3EventNotificationEncoder
  [in out ctx]
  (let [env-map       (env)
        input-objects (json->objects in)]
    (info "/tmp has" (str (float (/ (.getUsableSpace (io/file "/tmp")) 1024 1024)) "mb") "left")
    (info "/tmp has the following files:")
    (doseq [file (file-seq (io/file "/tmp"))]
      (info (.getAbsolutePath file)))
    (clean-tmp)
    (try
      (with-open [lambda-output (io/writer out)]
        (doseq [^java.io.File file (map download-object input-objects)]
          (try
            (process-file file env-map)
            (finally
              (debug "deleting" file)
              (.delete file))))
        (json/generate-stream input-objects lambda-output))
      (finally
        (clean-tmp)))))
