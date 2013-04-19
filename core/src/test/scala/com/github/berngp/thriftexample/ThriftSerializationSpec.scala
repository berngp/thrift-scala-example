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

import com.github.berngp.thriftexample.thrift.example.BinPacket

/** */
class ThriftSerializationSpec extends FlatSpec with SequentialNestedSuiteExecution {

  behavior of "Thrift Serialization for Hadoop"

  object buffer {
  }

  object fixture {
  }

  it should "recognize a Thrift object as Serializable" in {
    val ser = new ThriftSerialization[BinPacket]
    ser.accept(classOf[BinPacket]) should be (true)
  }

  it should "serialize a Thrift object to HDFS" in (pending)

  it should "deserialize a Thrift object from HDFS" in (pending)

  it should "be loadable to Hadoops Serializers" in  (pending)

  it should "offer a way to transform a Thrift TBase to a Hadoop Writable" in (pending)

  it should "serialize a Thrift Hadoop Writable" in (pending)

  it should "deserialize a Thrift Hadoop Writable" in (pending)


}
