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
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.{SequentialNestedSuiteExecution, FlatSpec}


/** */
class HadoopSequenceFileWriterSpec extends FlatSpec with SequentialNestedSuiteExecution {

  import HadoopSequenceFileWriter._

  object buffer {
    var builderBox: Option[HadoopSequenceFileWriterBuilder[PRESENT, _ <: BUILDER_REQ]] = None
  }

  behavior of "A Hadoop Writer Builder"

  object fixtures {
    val conf = new HadoopConf()
    val fs = new LocalFileSystem(HadoopFileSystem.get(conf))
    val filePath = "./target/sequence-file-writer"
  }

  it should "instantiate a HDFS Sequence File Writer with a known Hadoop Conf" in {
    buffer.builderBox = Some(hdfsWriter() withHadoopConf fixtures.conf)
    buffer.builderBox should be('defined)
  }

  it should "write to a _Sequence Writer_ created from the builder" in {
    val path = fixtures.filePath + "/writer.text"
    val writerBox = buffer.builderBox.get withFile (path) build() asSequenceFileWriter()
    writerBox should be('defined)

    val writer = writerBox.get
    writer.append(Nil.toWritable(), "value".toWritable())
    writer.close()

    fixtures.fs.exists(new Path(path)) should be(true)
  }

  it should "write to a _Sequence Writer_ given by a Monad that closes the writer" in {
    val path = fixtures.filePath + "/do-with-writer.text"
    buffer.builderBox.get withFile (path) build() doWithSequenceFileWriter {
      writer =>
        writer.append(Nil.toWritable(), "value".toWritable())
    } match {
      case f: Failure =>
        fail(f.msg, f.exception.openOr(new IllegalStateException("Exception expected!")))
      case _ =>
    }
    fixtures.fs.exists(new Path(path)) should be(true)
  }
}
