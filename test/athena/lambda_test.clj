(ns athena.lambda-test
  (:require [athena.lambda :refer :all]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))


(deftest env-test
  (is (= {:orc-schema "struct<x:decimal(12,2)>"} (env {"ORC_SCHEMA" "struct<x:decimal(12,2)>"})))
  (is (= {:orc-schema "struct<x:decimal(12,2)>"} (env {"ORC_SCHEMA" "struct<x:decimal(12,2)>" "DESTINATION_S3_BUCKET" ""}))))

(deftest json->objects-test
  (is (= [{:key "searches.json" :bucket-name "sourcebucket"}] (json->objects (io/resource "s3_notification.json")))))

(deftest output-path-test
  (is (= (io/file "/tmp/foo.txt.orc") (output-file "/tmp" "~/Foo/foo.txt")))
  (is (= (io/file "/tmp/foo.txt.orc") (output-file "/tmp" "~/Foo/foo.txt.gz")))
  (is (= (io/file "/tmp/foo.orc") (output-file "/tmp" "foo.sz")))
  (is (= (io/file "/tmp/foo.orc") (output-file "/tmp" "foo.gzip")))
  (is (= (io/file "/tmp/foo.orc") (output-file "/tmp" "foo.bzip2")))
  (is (= (io/file "/tmp/foobzip2.orc") (output-file "/tmp" "foobzip2.gz")))
  (is (= (io/file "foobzip2.orc") (output-file nil "foobzip2.gz"))))

(deftest partition-fn-test
  (is (nil? (partition-fn {})))
  (is (= :field (partition-fn {:partition-by "field" :partition-key "dt"})))
  (is (= :field (partition-fn {:partition-by ":field" :partition-key "dt"})))
  (is (= 11 ((partition-fn {:partition-by "(fn [x] (+ x 10))" :partition-key "dt"}) 1))))

(deftest partition-key-test
  (is (= {:dt :requested_at} (partition-key {:partition-by ":requested_at" :partition-key "dt"}))))
