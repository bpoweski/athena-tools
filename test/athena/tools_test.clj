(ns athena.tools-test
  (:require [clojure.test :refer :all]
            [athena.tools :as tools :refer :all]
            [orca.core :as orc]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (org.apache.orc TypeDescription)))


(deftest compression-encoding-test
  (is (= :snappy-framed (compression-encoding (io/resource "simple.json.sz"))))
  (is (= :gzip (compression-encoding (io/resource "simple.json.gz"))))
  (is (= :bzip2 (compression-encoding (io/resource "simple.json.bz2"))))
  (is (nil? (compression-encoding (io/resource "simple.json")))))

(deftest ecape-schema-test
  (is (= "`_id`" (escape-field "_id")))
  (is (= "`_meta`" (escape-field "_meta")))
  (is (= "`question?`" (escape-field "question?")))
  (is (= "array<struct<id:long,`_meta`:map<string,string>>>" (escape-schema "array<struct<id:long,_meta:map<string,string>>>"))))

(deftest parse-opts-test
  (is (= {:output "out.orc"} (parse-opts ["-o" "out.orc"]))))
