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

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import net.liftweb.common.{Failure, Full, Empty}


/** */
class ThriftProducerSpec extends FlatSpec {


  behavior of "A Thrift Producer"

  val kNetRecord = NetRecord(1, 2, 3, 4, 5, 6, 7)

  import ThriftProducer.NetRecordToThrift

  it should "transform a _NetRecord_ to its thrift equivalent" in {
    kNetRecord.asThriftBox() match {
      case Full(thriftRecord) =>
        thriftRecord.getFlowSetId() should equal (kNetRecord.flowSetId)
        thriftRecord.getTemplateId() should equal (kNetRecord.templateId)
        thriftRecord.getIpV4SrcAddr() should equal (kNetRecord.ipV4SrcAddr)
        thriftRecord.getIpV4DstAddr() should equal (kNetRecord.ipV4DstAddr)
        thriftRecord.getIpV4NextHop() should equal (kNetRecord.ipv4NextHop)
        thriftRecord.getInBytes() should equal (kNetRecord.inBytes)
        thriftRecord.getInPkts() should equal (kNetRecord.inPkts)
      case Empty =>
        fail("A Thrift Net Record equivalent was expected but none obtained.")
      case Failure(m, t, c) =>
        fail("Shouldn't Fail: " + m, t.openOr(new Exception(m)))
    }

  }

  val kNetPacketHeader = NetPacketHeader(1,2,3,4,5)

  import ThriftProducer.NetPacketHeaderToThrift

  it should "transform a _Net Packet Header_ to its thrift equivalent" in {
    kNetPacketHeader.asThriftBox() match {
      case Full(thriftHeader) =>
        thriftHeader.getVersion() should equal (kNetPacketHeader.version)
        thriftHeader.getSequenceNumber() should equal (kNetPacketHeader.sequenceNumber)
        thriftHeader.getSourceId() should equal (kNetPacketHeader.sourceId)
        thriftHeader.getSysUpTime() should equal (kNetPacketHeader.sysUpTime)
        thriftHeader.getUnixSecs() should equal (kNetPacketHeader.unixSecs)
        thriftHeader.getCount() should equal (0)
      case Empty =>
        fail("A Thrift Net Packet Header equivalent was expected but none obtained.")
      case Failure(m,t,c) =>
        fail("Shouldn't Fail: " + m, t.openOr(new Exception(m)))
    }
  }

  val kRecordList = List(kNetRecord)
  val kNetBinPacket = NetBinPacket(kNetPacketHeader, kRecordList)

  import ThriftProducer.NetBinPacketToThrift

  it should "transform a _Net Bin Packet_ to its thrift equivalent" in {
    kNetBinPacket.asThriftBox() match {
      case Full(binPacket) =>
        binPacket.getHeader().getCount() should equal (kRecordList.size)
        binPacket.getRecords().size() should equal (kRecordList.size)

      case Empty =>
        fail("A Thrift Net Packet Header equivalent was expected but none obtained.")

      case Failure(m,t,c) =>
        fail("Shouldn't Fail: " + m, t.openOr(new Exception(m)))
    }
  }

}
