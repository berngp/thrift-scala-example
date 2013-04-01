/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 * 
 */

// include "com.github.berngp.thriftexample.thrift.shared"

/** Namespaces **/
namespace java   com.github.berngp.thriftexample.thrift.example

/** Current Version of _ Packet Header_.*/
const i16 CURRENT_EXAMPLE_PCKT_VERSION = 9 

/** Describes the _ Packet Header_, it should be acompanied by a list of _ Net Records_  */
struct PacketHeader{
    /** Given version of this Header Records, should be constant.*/
    1:required i16 version = CURRENT_EXAMPLE_PCKT_VERSION,
    /** Total number of Records inside the bin.*/
    2:required i32 count,
    /** Time in milliseconds since this device was first booted. */
    3:required i64 sysUpTime,
    /** Time in seconds since 0000 UTC 1970, at which the Export Packet leaves the Exporter. */
    4:required i32 unixSecs,
    /** 
     * Incremental sequence counter of all Export Packets sent from the current Observation Domain by the Exporter.
     * This value MUST be cumulative, and SHOULD be used by the Collector to identify whether any Export Packets have been missed.
     */
    5:required i64 sequenceNumber,
    /** A 32-bit value that identifies the Exporter Observation Domain. **/
    6:required i64 sourceId
}

struct NetRecord {
    1:required i16 flowSetId,  //= 0 |      Length = 28 bytes        |
    2:required i16 templateId, //= 256 |       Field Count = 5         |
    3:required i64 ipV4SrcAddr,//= 8 |       Field Length = 4        |
    4:required i64 ipV4DstAddr,//= 12|       Field Length = 4        |
    5:required i32 ipV4NextHop,//= 15|       Field Length = 4        |
    6:required i16 inPkts,     //= 2 |       Field Length = 4        |
    7:required i64 inBytes = 0
}

/** 
 * Inspired by reference http://www.ietf.org/rfc/rfc3954.txt  
 * 
 * Bin that stores the _ Packet Header_ as well as a 
 * list of _ Net Records_.
 *
 *   Thrift Object:
 *
 *   +--------+-----------------------------------. . .
 *   |        | +-----------------------+
 *   | Bin    | | List fo               |
 *   | Packet | |  Net Records          |   . . .
 *   | Header | |                       |
 *   |        | +-----------------------+
 *   +--------+-----------------------------------. . .
 *
 */
struct BinPacket {
    1:PacketHeader  header,
    3:list<NetRecord> records
}
