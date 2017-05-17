(ns athena.jdbc
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint print-table]])
  (:gen-class))


(defn jdbc-url [{:keys [bucket schema prefix credential-provider-args credential-provider]}]
  {:pref [(string? credential-provider)]}
  (let [s3-url (str "s3://" bucket "/" prefix)]
    (cond-> "jdbc:awsathena://athena.us-east-1.amazonaws.com:443"
      schema                             (str "/" schema)
      true                               (str "?s3_staging_dir=" s3-url)
      true                               (str "&aws_credentials_provider_class=" credential-provider)
      (string? credential-provider-args) (str "&aws_credentials_provider_arguments=" credential-provider-args))))

(defn query-ref
  "Executes query and returns an S3 URL pointing to the results"
  [sql {:keys [bucket prefix table] :as options}]
  {:pref (string? bucket) (string? prefix) (string? aws-provider)}
  (let [s3-url (str "s3://" bucket "/" prefix)]
    (with-open [conn (java.sql.DriverManager/getConnection (jdbc-url options))
                rs   (.executeQuery (.createStatement conn) sql)]
      (let [query-id (.getQueryExecutionId (.getClient rs))]
        {:bucket bucket :key (str prefix "/" query-id ".csv")}))))

(defn map->url [{:keys [bucket key]}]
  (str "s3://" bucket "/" key))

(defn sql-command [{:keys [options summary] :as opts}]
  (println (map->url (query-ref (or (:execute options) (slurp (:file options))) options))))
