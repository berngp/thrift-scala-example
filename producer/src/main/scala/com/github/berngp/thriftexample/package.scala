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

package com.github.berngp

import org.apache.hadoop.io.{Writable, SequenceFile}
import java.io.IOException
import collection.GenSeq


package object thriftexample {

  sealed trait NetStructure

  /** */
  case class NetBinPacket(header: NetPacketHeader, records: List[NetRecord]) extends  NetStructure

  /** */
  case class NetPacketHeader(version: Short, sysUpTime: Long, unixSecs: Int, sequenceNumber: Long, sourceId: Int) extends  NetStructure

  /** */
  case class NetRecord(flowSetId: Short, templateId: Short, ipV4SrcAddr: Long, ipV4DstAddr: Long, ipv4NextHop: Int, inPkts: Short, inBytes:Int) extends  NetStructure

  //def actor(actor: => Actor, name: Option[String] = None, dispatcherName: Option[String] = None)(implicit actorRefFactory: ActorRefFactory): ActorRef = {

  case class SequenceFileWriterRunner[K<:Writable,V<:Writable](writer:SequenceFile.Writer, seq : Seq[(K,V)] = Seq.empty ) {

    @throws[IOException]
    def _dumpSeqToWriter(s : GenSeq[(K,V)]) =
      s.foreach ( (e) =>  writer.append(e._1,e._2))

    @throws[IOException]
    def run() = _dumpSeqToWriter(seq)

    def parRun() = _dumpSeqToWriter(seq.par)
  }


}
