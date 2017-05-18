# Athena Tools

A set of tools to accomplish the following:

* Creationg of Apache ORC files
* Inferring a schema from JSON
* A Lambda function that will encode S3 objects as ORC via S3 notification.
* Simple Athena SQL execution from the command line

## ORC Encoding via AWS Lambda

The AWS prescribed method for encoding S3 data into an efficient data format for Athena is [awful](https://docs.aws.amazon.com/athena/latest/ug/convert-to-columnar.html).
Rarely does one want the first instruction of anything to involve creating a Hadoop cluster.

### ORC S3 Notification Encoder

 | Environment Variable   | Description                                             |
 | ---------------------- | ------------------------------------------------------- |
 | DESTINATION_S3_BUCKET  | Bucket here the ORC files will be stored.               |
 | DESTINATION_S3_PREFIX  | Prefix to add to the S3 key                             |
 | PARTITION_BY           | Optional fn to partition rows by.  Evaled Clojure code. |
 | PARTITION_KEY          | Name of the variable used as part of the parition       |


## Usage

FIXME

## Build Prerequisites

Download the jar because the Athena team hates maven.

```bash
$ aws s3 cp s3://athena-downloads/drivers/AthenaJDBC41-1.0.1.jar .
```

Install it into your local repo.

```bash
$ mvn install:install-file \
  -Dfile=AthenaJDBC41-1.0.1.jar \
  -DgroupId=com.amazonaws \
  -DartifactId=athena-jdbc41 \
  -Dversion=1.0.1 \
  -Dpackaging=jar
```

## License

Copyright © 2017 Ben Poweski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
