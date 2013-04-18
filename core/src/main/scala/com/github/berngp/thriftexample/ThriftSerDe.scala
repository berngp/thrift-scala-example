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

import java.util.Properties
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.SerDeException
import org.apache.hadoop.hive.serde2.SerDeStats
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.io.Writable
import org.apache.thrift.{TFieldIdEnum, TBase}


class ThriftSerDe[T <: org.apache.thrift.TBase[org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]] extends SerDe {

  @GuardedBy(value = "initLock")
  var inspector: ObjectInspector = null
  val initLock: ReentrantLock = new ReentrantLock()

  @throws[SerDeException]
  def deserialize(w: Writable): AnyRef = {
    if (w.isInstanceOf[ThriftWritableAdapter[T]]) {
      (w.asInstanceOf[ThriftWritableAdapter[T]]).getFromBytes
    } else {
      throw new SerDeException(s"Not an instance of ThriftHadoopWritable or accepted by the ThriftSerialization: $w")
    }
  }

  def getObjectInspector: ObjectInspector = inspector

  override def initialize(conf: Configuration, p: Properties) = {
    _setInspector(_getThriftClass(conf, _getThriftClassName(p)))
  }

  @throws[SerDeException]
  private def _getThriftClassName(p: Properties): String = {
    val thriftClassName = p.getProperty(serdeConstants.SERIALIZATION_CLASS, null);
    if (thriftClassName == null) {
      throw new SerDeException(s"Required property ${serdeConstants.SERIALIZATION_CLASS} is null.")
    }
    thriftClassName
  }

  @throws[SerDeException]
  private def _getThriftClass(conf: Configuration, thriftClassName: String): Class[_] = {
    var thriftClass: Class[_] = null
    try {
      thriftClass = conf.getClassByName(thriftClassName)
    } catch {
      case e: ClassNotFoundException =>
        throw new SerDeException("Failed getting class for " + thriftClassName)
    }
    thriftClass
  }

  private def _setInspector(thriftClass: Class[_]): Unit = {
    if (initLock.tryLock()) {
      try {
        inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          thriftClass, ObjectInspectorFactory.ObjectInspectorOptions.THRIFT)
      } finally {
        initLock.unlock()
      }
    } else {
      throw new IllegalStateException("Concurrent initialization detected on the same reference, we were unable to obtain the inspector lock.")
    }
  }

  /** @todo implement something meaningful. */
  def getSerializedClass: Class[_ <: Writable] = classOf[ThriftWritableAdapter[T]]

  /** @todo implement something meaningful. */
  def serialize(p1: Any, p2: ObjectInspector): Writable = null

  /** @todo implement something meaningful. */
  def getSerDeStats: SerDeStats = null

}
