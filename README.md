# Athena Tools

A set of tools to accomplish the following:

* CLI ORC file encoding
* ORC schema inference
* Lambda function that will encode S3 objects as Lambda from an S3 notification.


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

## License

Copyright © 2017 Ben Poweski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
