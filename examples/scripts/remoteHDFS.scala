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
import com.github.berngp.thriftexample.thrift.example.{NetRecord => ThriftNetRecord}
import java.net.URI
import java.util.Date
import net.liftweb.common.Failure
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable.TreeMap

val conf = new HadoopConf()

/*
"core-site.xml hdfs-site.xml".split(' ').foreach(r =>
  conf.addResource(new Path(s"examples/etc/hadoop/$r"))
)
*/

/*
TODO add a UserGroupHelper to the DSL.
want to do something like ` asUser "my-example" ` and ` withUser "my-example" {  => } `
UserGroupInformation ugi = UserGroupInformation.createProxyUser("hduser", UserGroupInformation.getLoginUser());

```java
ugi.doAs(new PrivilegedExceptionAction() {
public Void run() throws Exception {
  Configuration jobconf = new Configuration();
  jobconf.set("fs.default.name", "hdfs://server:hdfsport");
  jobconf.set("hadoop.job.ugi", "hduser");
  jobconf.set("mapred.job.tracker", "server:jobtracker port");
  String[] args = new String[] { "data/input", "data/output" };
  ToolRunner.run(jobconf, WordCount.class.newInstance(), args);
  return null;
} });
```
 */

System.setProperty("HADOOP_PROXY_USER", "my-example")

val fs: FileSystem = FileSystem.get(URI.create("hdfs://nn.hadoop-ch4-mapred-hive.local:8020/"), conf, "my-example")
fs.getStatus.getCapacity

// TODO add a configuration helper to set variables and to add values to current collections. e.g. io.serializations
Console.println(Console.MAGENTA + s"Default io.serializations ${conf.get("io.serializations")}" + Console.WHITE)
conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization,com.github.berngp.thriftexample.ThriftSerialization")
conf.set("fs.defaultFS", "hdfs://nn.hadoop-ch4-mapred-hive.local:8020")

// TODO clean the way we generate Destination and Source Addresses, consider using Ranges as native elements of IPv4Address e.g. IPv4AddressRange
val destinationAddresses = (1 to 100).map("192.168.1." + _).map(IPv4Address(_)).map(_.get).toSet
val sourceAddresses = (1 to 254).map("192.168.100." + _).map(IPv4Address(_)).map(_.get).toSet

val format = new java.text.SimpleDateFormat("yyyyMMdd_Hms")
val seqFilesDir = s"/user/my-example/script-thrift-example/${format.format(new Date)}"

Console.println(Console.GREEN + s"Writing to ${seqFilesDir}" + Console.WHITE)

val plan = builder() withDestinationsAddresses destinationAddresses withSourceAddresses sourceAddresses withSize 10 withVoter Voting.constant plan()

val b = hdfsWriter() withHadoopConf conf withReplication 1

plan.timeSeries.view.foreach {
  bin =>
  // Creating the Meta from the Bin Header info.
    val meta = TreeMap[String, String](
      "sequenceNumber" -> bin.header.sequenceNumber.toString,
      "sourceId" -> bin.header.sourceId.toString,
      "sysUpTime" -> bin.header.sysUpTime.toString
    )

    Console.println(Console.WHITE + s"Writing for bin ${meta}" + Console.WHITE)

    //Writing the Sequence Files with bare thrift records.
    b withMeta meta withFile s"${seqFilesDir}/thrift-NetRecords/${bin.header.sequenceNumber}.seq" withValueClass classOf[ThriftNetRecord] build() doWithSequenceFileWriter {
      writer =>
        bin.records.view.map(_.asThriftBox()).filter(_.isDefined).map(_.get).foreach(r =>
          writer.append(Nil.toWritable(), r)
        )
    } match {
      case f: Failure =>
        Console.err.println(f.messageChain)
        f.exception.get.printStackTrace()
      case _ =>
    }

    // Writting the Sequence files with Writables.
    b withMeta meta withFile s"${seqFilesDir}/thrift-WritableNetRecords/${bin.header.sequenceNumber}.seq" withValueClass classOf[WritableThriftNetRecord] build() doWithSequenceFileWriter {
      writer =>
        bin.records.view.map(_.asThriftBox()).filter(_.isDefined).map(_.get).foreach(r =>
          writer.append(Nil.toWritable(), r.toWritable())
        )
    } match {
      case f: Failure =>
        Console.err.println(f.messageChain)
        f.exception.get.printStackTrace()
      case _ =>
    }
}
