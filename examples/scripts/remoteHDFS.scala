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
import com.github.berngp.thriftexample.thrift.example.{BinPacket => ThriftBinPacket}
import java.net.URI
import java.util.Date
import net.liftweb.common.Failure
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.FileSystem

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
conf.set("io.serializations", conf.get("io.serializations") + ",com.github.berngp.thriftexample.ThriftSerialization")
conf.set("fs.defaultFS", "hdfs://nn.hadoop-ch4-mapred-hive.local:8020")

// TODO clean the way we generate Destination and Source Addresses, consider using Ranges as native elements of IPv4Address e.g. IPv4AddressRange
val destinationAddresses = (1 to 50).map("192.168.1." + _).map(IPv4Address(_)).map(_.get).toSet
val sourceAddresses = (1 to 254).map("192.168.100." + _).map(IPv4Address(_)).map(_.get).toSet

val format = new java.text.SimpleDateFormat("yyyy-MM-dd-Hms")
val seqFilesDir = s"/user/my-example/script-thrift-example/${format.format(new Date)}"

Console.println(Console.GREEN + s"Writing to ${seqFilesDir}" + Console.WHITE)

val plan = builder() withDestinationsAddresses destinationAddresses withSourceAddresses sourceAddresses withSize 500 withVoter Voting.constant plan()

val thrifts = plan.timeSeries.map(_.asThriftBox()).filter(_.isDefined).map(_.get)

val b = hdfsWriter() withHadoopConf conf withValueClass classOf[ThriftBinPacket] withReplication (1)

thrifts.view.zipWithIndex.foreach {
  case (bin: ThriftBinPacket, index: Int) =>
    b withFile s"${seqFilesDir}/thrift-BinPacket/${index}.seq" withValueClass classOf[ThriftBinPacket] build() doWithSequenceFileWriter {
      writer =>
        writer.append(Nil.toWritable(), bin)
    } match {
      case f: Failure =>
        Console.err.println(f.messageChain)
        f.exception.get.printStackTrace()
      case _ =>
    }
    b withFile s"${seqFilesDir}/thrift-WritableBinPacket/${index}.seq" withValueClass classOf[WritableThriftBinPacket] build() doWithSequenceFileWriter {
      writer =>
        writer.append(Nil.toWritable(), bin.toWritable())
    } match {
      case f: Failure =>
        Console.err.println(f.messageChain)
        f.exception.get.printStackTrace()
      case _ =>
    }
}
