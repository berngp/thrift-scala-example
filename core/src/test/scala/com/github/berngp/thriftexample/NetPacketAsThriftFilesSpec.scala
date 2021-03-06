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

import net.liftweb.common.Failure
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem, LocalFileSystem, Path}
import org.apache.hadoop.io.SequenceFile
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers._
import thrift.example.{BinPacket => ThriftBinPacket}


class NetPacketAsThriftFilesSpec extends FlatSpec with SequentialNestedSuiteExecution {

  behavior of "Interaction between a Net Packet Traffic Generator and Thrift SequenceFiles HDFs Writers"

  import HadoopSequenceFileWriter._
  import NetPacketThriftGateway._
  import NetPacketTrafficGenerator._

  object buffer {
    var builderBox: Option[NetTrafficPacketPlanBuilder[_ <: NetPacketTrafficGenerator.BUILDER_REQ, _ <: NetPacketTrafficGenerator.BUILDER_REQ]] = None
    var thrifts: Option[Seq[ThriftBinPacket]] = None
  }

  def hadoopConf() = {
    val conf = new HadoopConf()
    conf.set("io.serializations", Seq(
      "org.apache.hadoop.io.serializer.WritableSerialization"
      , "com.github.berngp.thriftexample.ThriftSerialization"
    ).reduce(_ + "," + _))
    conf
  }

  object fixtures {
    val runIdentifier = java.util.UUID.randomUUID
    val destinationAddresses = (1 to 10).map("192.168.1." + _).map(IPv4Address(_)).map(_.get).toSet
    val sourceAddresses = (1 to 10).map("192.168.100." + _).map(IPv4Address(_)).map(_.get).toSet
    val timeSeriesSize = 10
    val conf = hadoopConf()
    val fs = new LocalFileSystem(HadoopFileSystem.get(conf))
    val seqFilesDir = new Path(s"./target/hdfs/net-packets-thrift/${runIdentifier}")
  }

  it should "instantiate a hdfsWriter" in {
    buffer.builderBox = Some(builder())
    buffer.builderBox should be('defined)
  }

  it should "be cable to generate a Time Series and encode it to Thrift Objects" in {
    val b = buffer.builderBox.get
    val g = b
      .withDestinationsAddresses(fixtures.destinationAddresses)
      .withSourceAddresses(fixtures.sourceAddresses)
      .withSize(fixtures.timeSeriesSize)
      .withVoter(Voting.constant)
      .build()

    val p = g.getPlan
    val thrifts = p.timeSeries.map(_.asThriftBox()).filter(_.isDefined).map(_.get)

    thrifts.size should be(p.timeSeries.size)
    buffer.thrifts = Some(thrifts)
  }

  it should "persist the Thrift Objects into Sequence Files using the ThriftSerialization" in {
    val b = hdfsWriter() withHadoopConf fixtures.conf withValueClass classOf[ThriftBinPacket]
    buffer.thrifts.get.view.zipWithIndex.foreach {
      t =>
        val a = b withFile (fixtures.seqFilesDir + s"/ThriftBinPacket/${t._2}.seq")
        a build() doWithSequenceFileWriter {
          writer =>
            writer.append(Nil.toWritable(), t._1)
        } match {
          case f: Failure =>
            fail(f.msg, f.exception.openOr(new IllegalStateException("Exception expected!")))
          case _ =>
        }
    }
  }

  it should "read the Thrift Objects contained in the SequenceFiles using the ThriftSerialization" in {
    buffer.thrifts.get.view.zipWithIndex.foreach {
      case (t, index) =>
        val path = s"${fixtures.seqFilesDir}/ThriftBinPacket/${index}.seq"
        val optFile = SequenceFile.Reader.file(new Path(path))
        val seqReader = new SequenceFile.Reader(fixtures.conf, optFile)

        val key = seqReader.next(Nil.toWritable())
        key should be (true)

        val stored = new ThriftBinPacket()
        seqReader.getCurrentValue(stored)

        stored should be(t)
    }
  }

  it should "persist the Thrift Objects into SequenceFiles usintg the WritableThriftBinPacket" in {
    val filePath = s"${fixtures.seqFilesDir}/WritableThriftBinPacket"
    info(s"Writting to path ${filePath}")

    // Configure the Builder
    val b = hdfsWriter() withHadoopConf fixtures.conf withValueClass classOf[WritableThriftBinPacket]

    buffer.thrifts.get.view.zipWithIndex.foreach {
      case (t, index) =>
        val a = b withFile s"${filePath}/${index}.seq"
        a build() doWithSequenceFileWriter {
          writer =>
            writer.append(Nil.toWritable(), t.toWritable())
            writer.hsync()
        } match {
          case f: Failure =>
            fail(f.msg, f.exception.openOr(new IllegalStateException("Exception expected!")))
          case _ =>
        }
    }
  }

  it should "read the SequenceFiles using the WritableThriftBinPacket" in {
    val filePath = s"${fixtures.seqFilesDir}/WritableThriftBinPacket"
    info(s"Reading files from path ${filePath}")

    buffer.thrifts.get.view.zipWithIndex.foreach {
      case (t, index) =>
        val path = s"${filePath}/${index}.seq"

        val optFile = SequenceFile.Reader.file(new Path(path))
        val seqReader = new SequenceFile.Reader(fixtures.conf, optFile)

        val key = seqReader.next(Nil.toWritable())
        key should be (true)

        val stored = new WritableThriftBinPacket()
        seqReader.getCurrentValue(stored)

        stored should be(t)
    }
  }

}
