(defproject athena-tools "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://github.com/bpoweski/athena-tools"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.trace "0.7.9"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 [com.fzakaria/slf4j-timbre "0.3.5"]
                 [cheshire "5.7.0"]
                 [org.xerial.snappy/snappy-java "1.1.4-M3"]
                 [org.apache.commons/commons-compress "1.13"]
                 [org.clojure/tools.trace "0.7.9"]
                 [mvxcvi/puget "1.0.1"]
                 [com.taoensso/timbre "4.8.0"]
                 [orca "0.1.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]
                 [com.amazonaws/aws-lambda-java-core "1.1.0"]
                 [com.amazonaws.athena.jdbc/athena-jdbc41 "1.0.1"]
                 [amazonica "0.3.94" :exclusions [com.google.protobuf/protobuf-java
                                                  com.amazonaws/amazon-kinesis-client
                                                  com.amazonaws/aws-java-sdk]]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.109"]
                 [com.amazonaws/aws-java-sdk-lambda "1.11.109"]
                 [com.taoensso/nippy "2.13.0"]]
  :profiles {:dev {:resource-paths ["test-resources"]}
             :uberjar {:aot :all}}
  :jvm-opts ["-Xmx1324m"]
  :uberjar-name "athena-tools-standalone.jar"
  :main athena.tools)
