
CREATE SCHEMA IF NOT EXISTS my_thrift_example;

USE my_thrift_example;

CREATE external TABLE netflow_records
  COMMENT 'Stores NetRocords wrapped in com.github.berngp.thriftexample.ThriftBytesWritable.'
  ROW format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
  WITH serdeproperties(
    "serialization.class"="com.github.berngp.thriftexample.thrift.example.NetRecord",
    "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol")
  stored AS
    sequencefile
;


LOAD DATA INPATH '/user/my-example/script-thrift-example/20130419_135757/thrift-ByteWritableNetRecords' OVERWRITE INTO TABLE netflow_records;


