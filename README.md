# Athena Tools

A set of tools to accomplish the following:

* CLI ORC file encoding
* ORC schema inference
* Lambda ORC encoding


## ORC Encoding via AWS Lambda

The AWS prescribed method for encoding S3 data into an efficient data format for Athena is [awful](https://docs.aws.amazon.com/athena/latest/ug/convert-to-columnar.html).
Rarely does one want the first instruction of anything to involve creating a Hadoop cluster.

### ORC Lambda Encoder

 | Environment Variable   | Description                               |
 | ---------------------- | ----------------------------------------- |
 | DESTINATION_S3_BUCKET  |    |
 | DESTINATION_S3_PREFIX  |   |


## Usage

FIXME

## License

Copyright © 2017 Ben Poweski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
