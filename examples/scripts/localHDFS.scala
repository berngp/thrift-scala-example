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


import com.github.berngp.thriftexample.HadoopSequenceFileWriter._
import com.github.berngp.thriftexample.NetPacketThriftGateway._
import com.github.berngp.thriftexample.NetPacketTrafficGenerator._
import com.github.berngp.thriftexample._
import com.github.berngp.thriftexample.thrift.example.BinPacket
import net.liftweb.common.Failure
import org.apache.hadoop.conf.{Configuration => HadoopConf}

val conf = new HadoopConf()
/*
"core-site.xml hdfs-site.xml".split(' ').foreach(r =>
  conf.addResource(new Path(s"examples/etc/hadoop/$r"))
)
*/
conf.set("io.serializations", conf.get("io.serializations") + ",com.github.berngp.thriftexample.ThriftSerialization")

val destinationAddresses = (1 to 10).map("192.168.1." + _).map(IPv4Address(_)).map(_.get).toSet
val sourceAddresses = (1 to 10).map("192.168.100." + _).map(IPv4Address(_)).map(_.get).toSet
val seqFilesDir = "./target/hdfs/script-thrift"

val plan = builder() withDestinationsAddresses destinationAddresses withSourceAddresses sourceAddresses withSize 500 withVoter Voting.constant plan()

val thrifts = plan.timeSeries.map(_.asThriftBox()).filter(_.isDefined).map(_.get)

val b = hdfsWriter() withHadoopConf conf withValueClass classOf[BinPacket]

thrifts.view.zipWithIndex.foreach {
  case (bin: BinPacket, index: Int) =>
    b withFile (seqFilesDir + s"/ThriftBinPacket/${index}") build() doWithSequenceFileWriter {
      writer =>
        writer.append(Nil.toWritable(), bin)
    } match {
      case f: Failure =>
        Console.err.println(f.messageChain)
        f.exception.get.printStackTrace()
      case _ =>
    }
}
