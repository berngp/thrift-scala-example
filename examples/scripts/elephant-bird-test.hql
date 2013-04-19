CREATE external TABLE elephantbird_test
  -- no need to specify a schema - it will be discovered at runtime
  -- (myint int, myString string, underscore_int int)
  partitioned by (dt string)
  -- hive provides a thrift deserializer
  row format serde "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer"
  with serdeproperties (
      -- full name of our thrift struct class
      "serialization.class"="org.apache.hadoop.hive.serde2.thrift.test.IntString",
      -- use the binary protocol
      "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol"
  )
  stored as
    -- elephant-bird provides an input format for use with hive
    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
    -- placeholder as we will not be writing to this table
    outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
