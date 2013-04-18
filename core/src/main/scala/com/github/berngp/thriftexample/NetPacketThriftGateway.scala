/*
 * Copyright 2012-2013 Bernardo Gomez Palacio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.berngp.thriftexample

import net.liftweb.common._
import net.liftweb.util.ControlHelpers
import scala.collection.JavaConversions._
import thrift.example.{
BinPacket => ThriftBinPacket,
NetRecord => ThriftNetRecord,
PacketHeader => ThriftNetHeader
}

object NetPacketThriftGateway extends ControlHelpers with Loggable {

  def netBinPacketToThrift(binPacket: NetBinPacket): Box[ThriftBinPacket] = {

    val headerBox = packetHeaderToThrift(binPacket.header)

    val recordsBox = netRecordsToThrift(binPacket.records)

    (headerBox, recordsBox) match {
      case (Full(header: ThriftNetHeader), Full(records: List[ThriftNetRecord])) =>
        header.setCount(records.size)
        Full(new ThriftBinPacket(header, records))
      case (Full(header: ThriftNetHeader), Empty) =>
        Full(new ThriftBinPacket(header, List()))
      case (Empty, Empty) =>
        Empty
      case (Empty, _) =>
        Failure("The Header can't be empty")
      case (f1: Failure, f2: Failure) =>
        new Failure("Unable to transform the Header nor the Records to their Thrift Equivalents.", Empty, Empty)
      case (f1: Failure, _) =>
        new Failure("Unable to transform the Header to its Thrift Equivalent.", Empty, Empty)
      case (_, f1: Failure) =>
        new Failure("Unable to transform the Records to its Thrift Equivalent.", Empty, Empty)
      case (_, _) =>
        Failure("Unable to transform to Thrift Equivalent")
    }
  }

  def packetHeaderToThrift(header: NetPacketHeader): Box[ThriftNetHeader] =
    tryo(new ThriftNetHeader(header.version, 0, header.sysUpTime, header.unixSecs, header.sequenceNumber, header.sourceId))

  def netRecordsToThrift(netRecords: List[NetRecord] = List.empty): Box[List[ThriftNetRecord]] =
    tryo(netRecords.map(
      r => new ThriftNetRecord(r.flowSetId, r.templateId, r.ipV4SrcAddr, r.ipV4DstAddr, r.ipv4NextHop, r.inPkts, r.inBytes)))

  def netRecordsToThrift(r: NetRecord): Box[ThriftNetRecord] =
    if (r == null)
      Empty
    else
      tryo(new ThriftNetRecord(r.flowSetId, r.templateId, r.ipV4SrcAddr, r.ipV4DstAddr, r.ipv4NextHop, r.inPkts, r.inBytes))

  implicit class NetPacketHeaderToThrift(val header: NetPacketHeader) {
    def asThriftBox(): Box[ThriftNetHeader] = packetHeaderToThrift(header)
  }

  implicit class NetRecordToThrift(val record: NetRecord) {
    def asThriftBox(): Box[ThriftNetRecord] = netRecordsToThrift(record)
  }

  implicit class NetBinPacketToThrift(val binPacket: NetBinPacket) {
    def asThriftBox(): Box[ThriftBinPacket] = netBinPacketToThrift(binPacket)
  }

  class WritableThriftBinPacket(thrift:ThriftBinPacket) extends ThriftBinPacket(thrift) with ThriftHadoopWritable[ThriftBinPacket, ThriftBinPacket._Fields] {
    def this() = {
      this( new ThriftBinPacket())
    }
  }

  object WritableThriftBinPacket {
    def apply(thrift:ThriftBinPacket) = new WritableThriftBinPacket(thrift)
  }

  implicit class ThriftBinPacketToHDFSWritable(thrift: ThriftBinPacket) {
    def toWritable():WritableThriftBinPacket = WritableThriftBinPacket(thrift)
  }

  class WritableThriftNetRecord(thrift:ThriftNetRecord) extends ThriftNetRecord(thrift) with ThriftHadoopWritable[ThriftNetRecord, ThriftNetRecord._Fields] {
    def this() = {
      this( new ThriftNetRecord())
    }
  }

  object WritableThriftNetRecord {
    def apply(thrift:ThriftNetRecord) = new WritableThriftNetRecord(thrift)
  }

  implicit class ThriftNetRecordToHDFSWritable(thrift: ThriftNetRecord) {
    def toWritable():WritableThriftNetRecord = WritableThriftNetRecord(thrift)
  }

}
