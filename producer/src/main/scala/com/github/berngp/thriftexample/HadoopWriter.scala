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


import scala.collection.JavaConversions._
import java.{util => jutil}

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.SequenceFile.Metadata
import collection.immutable.TreeMap
import collection.mutable
import collection.mutable.ArrayBuffer
import org.apache.hadoop.util.Progressable
import javax.management.monitor.StringMonitor
import net.liftweb.common.{Box, Failure, Full}


object HadoopWriter {

  case class HadoopWriterRecipe private[HadoopWriter](hadoopConf: HadoopConf, options: Seq[Writer.Option]) {
    def asWriter() = try {
      Full(SequenceFile.createWriter(hadoopConf, options: _*))
    } catch {
      case iae: IllegalArgumentException =>
        Failure("Illegal Argument found for the Sequence File Writer!", Full(iae), None)
      case e: Exception =>
        Failure("Unable to create a Sequence File Writer!", Full(e), None)
    }
  }

  abstract class TRUE

  abstract class FALSE

  class HadoopWriterBuilder[HC, HF] private[HadoopWriter](val hadoopConf: HadoopConf, val theFile: Either[String, Path])(
    var theMeta: Option[TreeMap[String, String]] = None,
    var theBufferSize: Option[Int] = None,
    var theBlockSize: Option[Long] = None,
    var theCompression: Option[CompressionType] = Some(CompressionType.NONE),
    var theReplication: Option[Short] = Some(0),
    var theKeyClass: Option[Class[_]] = Some(classOf[Text]),
    var theValueClass: Option[Class[_]] = Some(classOf[Text]),
    var theProgressableReporter: Option[Progressable] = None) {

    private def _builder(hConf: HadoopConf = hadoopConf, file: Either[String, Path] = theFile) = {
      new HadoopWriterBuilder[HC, HF](hConf, file)(_, _, _, _, _, _, _, _)
    }

    def withMeta(m: TreeMap[String, String]) =
      _builder()(Some(m), theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withBufferSize(b: Int) =
      _builder()(theMeta, Some(b), theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withBlockSize(b: Long) =
      _builder()(theMeta, theBufferSize, Some(b), theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withCompression(c: CompressionType) =
      _builder()(theMeta, theBufferSize, theBlockSize, Some(c), theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withReplication(r: Short) =
      _builder()(theMeta, theBufferSize, theBlockSize, theCompression, Some(r), theKeyClass, theValueClass, theProgressableReporter)

    def withKeyClass(k: Class[_]) =
      _builder()(theMeta, theBufferSize, theBlockSize, theCompression, theReplication, Some(k), theValueClass, theProgressableReporter)

    def withValueClass(v: Class[_]) =
      _builder()(theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, Some(v), theProgressableReporter)

    def withProgressable(p: Progressable) =
      _builder()(theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, Some(p))

    def options() = {
      val seqBuffer: ArrayBuffer[Writer.Option] = mutable.ArrayBuffer()

      theFile match {
        case Left(fileStr) =>
          seqBuffer += Writer.file(new Path(fileStr))
        case Right(path) =>
          seqBuffer += Writer.file(path)
      }

      if (theMeta.isDefined)
        seqBuffer += Writer.metadata(new Metadata(new jutil.TreeMap(theMeta.get.map(v => new Text(v._1) -> new Text(v._2)))))
      if (theBufferSize.isDefined)
        seqBuffer += Writer.bufferSize(theBufferSize.get)
      if (theBlockSize.isDefined)
        seqBuffer += Writer.blockSize(theBlockSize.get)
      if (theCompression.isDefined)
        seqBuffer += Writer.compression(theCompression.get)
      if (theReplication.isDefined)
        seqBuffer += Writer.replication(theReplication.get)
      if (theKeyClass.isDefined)
        seqBuffer += Writer.keyClass(theKeyClass.get)
      if (theValueClass.isDefined)
        seqBuffer += Writer.valueClass(theValueClass.get)
      if (theProgressableReporter.isDefined)
        seqBuffer += Writer.progressable(theProgressableReporter.get)

      seqBuffer.toSeq
    }
  }

  implicit def enableBuild(builder: HadoopWriterBuilder[TRUE, TRUE]) = new {
    def build() =
      new HadoopWriterRecipe(builder.hadoopConf, builder.options())
  }

  def builder(hadoopConf: HadoopConf, file: String): Box[HadoopWriterBuilder[TRUE, TRUE]] = builder(hadoopConf, Left(file))

  def builder(hadoopConf: HadoopConf, path: Path): Box[HadoopWriterBuilder[TRUE, TRUE]] = builder(hadoopConf, Right(path))

  def builder(hadoopConf: HadoopConf, file: Either[String, Path]): Box[HadoopWriterBuilder[TRUE, TRUE]] = {
    if (hadoopConf == null)
      return Failure("A Hadoop Configuration Reference is required!")

    file match {
      case Left(fileName) if (fileName == null || fileName.isEmpty) =>
        return Failure("A file path is required!")
      case Right(path) if (path == null) =>
        return Failure("A file path is required!")
      case _ =>
    }

    Full(new HadoopWriterBuilder[TRUE, TRUE](hadoopConf, file)())
  }

  implicit def stringToText(string:String) : Text = new Text(string)

  implicit class StringToHadoopText(string:String) {
    def toText() = new Text(string)
  }

}
