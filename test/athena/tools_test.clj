(ns athena.tools-test
  (:require [clojure.test :refer :all]
            [athena.tools :refer :all]
            [orca.core :as orc]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (org.apache.orc TypeDescription)))


(deftest compression-encoding-test
  (is (= :snappy-framed (compression-encoding (io/resource "simple.json.sz"))))
  (is (= :gzip (compression-encoding (io/resource "simple.json.gz"))))
  (is (= :bzip2 (compression-encoding (io/resource "simple.json.bz2"))))
  (is (nil? (compression-encoding (io/resource "simple.json")))))
