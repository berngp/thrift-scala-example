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


import collection.immutable.TreeMap
import collection.mutable
import collection.mutable.ArrayBuffer
import java.{util => jutil}
import net.liftweb.common._
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io._
import org.apache.hadoop.util.Progressable
import scala.Some
import scala.collection.JavaConversions._


object HadoopSequenceFileWriter {

  case class HadoopSequenceFileWriterRecipe private[HadoopSequenceFileWriter](hadoopConf: HadoopConf, options: Seq[Writer.Option])
    extends net.liftweb.util.ControlHelpers with Logger {

    def asSequenceFileWriter(): Box[SequenceFile.Writer] = try {
      Full(SequenceFile.createWriter(hadoopConf, options: _*))
    } catch {
      case iae: IllegalArgumentException =>
        Failure("Illegal Argument found for the Sequence File Writer!", Full(iae), None)
      case e: Exception =>
        Failure("Unable to create a Sequence File Writer!", Full(e), None)
    }

    def doWithSequenceFileWriter[A](f: SequenceFile.Writer => A): Box[A] = {
      asSequenceFileWriter() match {
        case Full(writer) =>
          val fBox = tryo[A](f(writer))
          tryo(writer.close()) match {
            case f: Failure =>
              warn("Failed to close the SequenceFile.Writer silently!", f)
            case _ =>
          }
          fBox
        case f: Failure =>
          f
        case Empty =>
          Empty
      }
    }
  }

  /**
   * Type used to mark an attribute of the hdfsWriter as required.
   * TODO Document how this works to ensure all required attributes are set before we generate a writer.
   */
  abstract class BUILDER_REQ

  /** */
  abstract class PRESENT extends BUILDER_REQ

  /** */
  abstract class MISSING extends BUILDER_REQ

  class HadoopSequenceFileWriterBuilder[HC <: BUILDER_REQ, HF <: BUILDER_REQ](val theHadoopConf: HadoopConf = null,
                                                                              val theFile: Option[Either[String, Path]] = None,
                                                                              val theMeta: Option[TreeMap[String, String]] = None,
                                                                              val theBufferSize: Option[Int] = None,
                                                                              val theBlockSize: Option[Long] = None,
                                                                              val theCompression: Option[CompressionType] = Some(CompressionType.BLOCK),
                                                                              val theReplication: Option[Short] = Some(0),
                                                                              val theKeyClass: Option[Class[_]] = Some(classOf[NullWritable]),
                                                                              val theValueClass: Option[Class[_]] = Some(classOf[Text]),
                                                                              val theProgressableReporter: Option[Progressable] = None) {

    private def _builder[HC <: BUILDER_REQ, HF <: BUILDER_REQ] = {
      new HadoopSequenceFileWriterBuilder[HC, HF](_, _, _, _, _, _, _, _, _, _)
    }

    def withHadoopConf(c: HadoopConf) =
      _builder[PRESENT, HF](c, theFile, theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)


    def withFile(f: Either[String, Path]): HadoopSequenceFileWriterBuilder[HC, PRESENT] =
      _builder[HC, PRESENT](theHadoopConf, Some(f), theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withFile(f: String): HadoopSequenceFileWriterBuilder[HC, PRESENT] = withFile(Left(f))

    def withFile(f: Path): HadoopSequenceFileWriterBuilder[HC, PRESENT] = withFile(Right(f))

    def withMeta(m: TreeMap[String, String]) =
      _builder[HC, HF](theHadoopConf, theFile, Some(m), theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withBufferSize(b: Int) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, Some(b), theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withBlockSize(b: Long) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, Some(b), theCompression, theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withCompression(c: CompressionType) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, theBlockSize, Some(c), theReplication, theKeyClass, theValueClass, theProgressableReporter)

    def withReplication(r: Short) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, theBlockSize, theCompression, Some(r), theKeyClass, theValueClass, theProgressableReporter)

    def withKeyClass(k: Class[_]) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, theBlockSize, theCompression, theReplication, Some(k), theValueClass, theProgressableReporter)

    def withValueClass(v: Class[_]) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, Some(v), theProgressableReporter)

    def withProgressable(p: Progressable) =
      _builder[HC, HF](theHadoopConf, theFile, theMeta, theBufferSize, theBlockSize, theCompression, theReplication, theKeyClass, theValueClass, Some(p))

    def options() = {
      val seqBuffer: ArrayBuffer[Writer.Option] = mutable.ArrayBuffer()

      if (theFile.isDefined) {
        theFile.get match {
          case Left(fileStr) =>
            seqBuffer += Writer.file(new Path(fileStr))
          case Right(path) =>
            seqBuffer += Writer.file(path)
        }
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

  implicit def enableSequenceFileWriterBuilder(builder: HadoopSequenceFileWriterBuilder[PRESENT, PRESENT]) = new {
    def build() = {
      new HadoopSequenceFileWriterRecipe(builder.theHadoopConf, builder.options())
    }
  }

  def hdfsWriter() =
    new HadoopSequenceFileWriterBuilder[MISSING, MISSING]()

  implicit def stringToText(string: String): Text = new Text(string)

  implicit class NilToHadoopWritable(nil: List[Nothing]) {
    def toWritable() = NullWritable.get()
  }

  implicit class StringToHadoopWritable(string: String) {
    def toWritable() = new Text(string)

    def toText() = toWritable()
  }

  implicit class LongToHadoopWritable(long: Long) {
    def toWritable() = new LongWritable(long)
  }

  implicit class ShortToHadoopWritable(short: Short) {
    def toWritable() = new ShortWritable(short)
  }

  implicit class MapToHadoopWritable[K <: Writable, V <: Writable](map: Map[K, V]) {
    def toWritable() = {
      val mapWritable = new MapWritable()
      mapWritable.putAll(map)
      mapWritable
    }
  }

}
