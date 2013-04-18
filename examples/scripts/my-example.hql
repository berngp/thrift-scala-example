
CREATE SCHEMA IF NOT EXISTS my_thrift_example;

USE my_thrift_example;

DROP TABLE bin_packets;

CREATE EXTERNAL TABLE bin_packets
  -- no need to specify a schema - it will be discovered at runtime
  -- (myint int, myString string, underscore_int int)
  partitioned by (yymmdd_hhMMss string)
  -- hive provides a thrift deserializer
  row format serde "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer"
  -- Serde Properties
  with serdeproperties (
      -- full name of our thrift struct class
      "serialization.class"="com.github.berngp.thriftexample.thrift.example.BinPacket",
      -- use the binary protocol
      "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol"
  )
  stored as
    -- elephant-bird provides an input format for use with hive
    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
    -- placeholder as we will not be writing to this table
    outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
;


ALTER TABLE bin_packets ADD PARTITION (yymmdd_hhMMss = '20130412_131733') 
LOCATION '/user/my-example/script-thrift-example/20130412_131733/thrift-BinPacket/';

--LOAD DATA INPATH '/user/my-example/script-thrift-example/20130412_135141/thrift-BinPacket/1.seq'
--INTO TABLE bin_packets PARTITION(yymmdd_hhMMss='20130412_135141');


DROP TABLE writable_bin_packets;

CREATE EXTERNAL TABLE writable_bin_packets
  -- no need to specify a schema - it will be discovered at runtime
  partitioned by (yymmdd_hhMMss string)
  -- hive provides a thrift deserializer
  row format serde "com.twitter.elephantbird.hive.serde.ThriftSerDe"
  with serdeproperties (
      -- full name of our thrift struct class
      "serialization.class"="com.github.berngp.thriftexample.NetPacketThriftGateway$WritableThriftBinPacket",
      -- use the binary protocol
      "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol"
  )
  stored as
    -- elephant-bird provides an input format for use with hive
    inputformat "com.twitter.elephantbird.mapred.input.HiveMultiInputFormat"
    -- placeholder as we will not be writing to this table
    outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    -- outputformat "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat "
-- Make sure that the hive user can write to the warehouse directory
-- LOCATION "/user/my-example/warehouse/my_thrift_example.db/writable_bin_packets"
;

-- Make sure that the my-example user can write to the warehouse directory.
ALTER TABLE bin_packets
    ADD PARTITION (yymmdd_hhMMss = "20130412_135141")
    LOCATION '/user/my-example/script-thrift-example/20130412_135141/thrift-WritableBinPacket';

--LOAD DATA INPATH '/user/my-example/script-thrift-example/20130412_135141/thrift-WritableBinPacket/1.seq' INTO TABLE my_thrift_example.writable_bin_packets 
--PARTITION(yymmdd_hhMMss='20130412_135141');


CREATE external TABLE netflow_records
  -- partitioned BY (yymmdd_hhMMss string)
  --
  ROW format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
  -- ROW format serde "com.twitter.elephantbird.hive.serde.ThriftSerDe"
  WITH serdeproperties(
    "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol",
    "serialization.class"="com.github.berngp.thriftexample.thrift.example.NetRecord"
  )
  stored as
    sequencefile
    --  textfile
    -- elephant-bird provides an input format for use with hive
    inputformat "com.twitter.elephantbird.mapreduce.input."
    -- placeholder as we will not be writing to this table
    -- outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    --outputformat "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"
;

-- Make sure that the my-example user can write to the warehouse directory.
ALTER TABLE netflow_records
    ADD PARTITION (yymmdd_hhMMss = "20130417_11511")
    LOCATION '/user/my-example/script-thrift-example/20130417_11511/thrift-NetRecords';


LOAD DATA INPATH '' INTO TABLE netflow_records;
--PARTITION(yymmdd_hhMMss='20130412_135141');


CREATE external TABLE w_netflow_records
  -- partitioned BY (yymmdd string)
  ROW format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
  -- ROW format serde "com.twitter.elephantbird.hive.serde.ThriftSerDe"
  WITH serdeproperties(
    -- "serialization.class"="com.github.berngp.thriftexample.NetPacketThriftGateway$WritableThriftNetRecord",
    "serialization.class"="com.github.berngp.thriftexample.thrift.example.NetRecord",
    "serialization.format"="org.apache.thrift.protocol.TBinaryProtocol")
  stored AS
    sequencefile
    -- elephant-bird provides an input format for use with hive
    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
    -- placeholder as we will not be writing to this table
    outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    -- outputformat "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"
;


-- Make sure that the my-example user can write to the warehouse directory.
ALTER TABLE w_netflow_records
    ADD PARTITION (yymmdd_hhMMss = "20130417_145127")
    LOCATION '/user/my-example/script-thrift-example/20130417_145127/thrift-WritableNetRecords';
    


LOAD DATA INPATH '' INTO TABLE w_netflow_records;

