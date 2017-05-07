(ns athena.lambda-test
  (:require [athena.lambda :refer :all]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))


(deftest env-test
  (is (= {:orc-schema "struct<x:decimal(12,2)>"} (env {"ORC_SCHEMA" "struct<x:decimal(12,2)>"}))))

(deftest json->objects-test
  (is (= [{:key "searches.json" :bucket-name "sourcebucket"}] (json->objects (io/resource "s3_notification.json")))))

(deftest output-path-test
  (is (= (io/file "/tmp/foo.txt.orc") (output-file "~/Foo/foo.txt")))
  (is (= (io/file "/tmp/foo.txt.orc") (output-file "~/Foo/foo.txt.gz")))
  (is (= (io/file "/tmp/foo.orc") (output-file "foo.sz")))
  (is (= (io/file "/tmp/foo.orc") (output-file "foo.gzip")))
  (is (= (io/file "/tmp/foo.orc") (output-file "foo.bzip2")))
  (is (= (io/file "/tmp/foobzip2.orc") (output-file "foobzip2.gz")))
  (is (= (io/file "foobzip2.orc") (output-file nil "foobzip2.gz"))))
