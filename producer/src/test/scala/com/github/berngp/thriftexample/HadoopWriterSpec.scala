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

import net.liftweb.common.{Box, Empty}
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem, Path}
import org.apache.hadoop.io.SequenceFile
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.{SequentialNestedSuiteExecution, FlatSpec}


/** */
class HadoopWriterSpec extends FlatSpec with SequentialNestedSuiteExecution {

  import HadoopWriter._

  object buffer {
    var builderBox: Option[HadoopWriterBuilder[_ <: BUILDER_REQ, _ <:BUILDER_REQ]] = None
    var writerBox: Box[SequenceFile.Writer] = Empty
  }

  behavior of "A Hadoop Writer Builder"

  object fixture {
    val conf = new HadoopConf()
    val fs = HadoopFileSystem.get(conf)
    val filePath = new Path("producer/target/hadoopWriterTest.txt")
  }

  it should "instantiate a builder" in {
    buffer.builderBox = Some( builder() )
    buffer.builderBox should be('defined)
  }

  it should "get the _Sequence Writer_ " in {
    buffer.writerBox = buffer.builderBox.get withHadoopConf fixture.conf withFile fixture.filePath build() asSequenceFileWriter()
    buffer.writerBox should be('defined)
  }

  it should "append values to the _Sequence Writer_ " in {
    val writer = buffer.writerBox.get
    writer.append("key".toText(), "value".toText())
    writer.close()

    val f = fixture
    f.fs.exists(f.filePath) should be(true)
  }

}
