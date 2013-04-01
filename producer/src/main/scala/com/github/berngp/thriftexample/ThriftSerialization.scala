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

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import org.apache.hadoop.io.serializer.{Serializer, Deserializer}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.thrift.{TFieldIdEnum, TBase, TException}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TIOStreamTransport


/** */
class ThriftSerialization[T <: TBase[_,_] : Manifest, F <: TFieldIdEnum] extends org.apache.hadoop.io.serializer.Serialization[T] {

  def accept(c: Class[_]): Boolean = c.isAssignableFrom(reflect.classTag[T].runtimeClass)

  def getSerializer(c: Class[T]): Serializer[T] = new TSerializerAdapter

  def getDeserializer(c: Class[T]): Deserializer[T] = new TDeserializerAdapter(c)

  class TSerializerAdapter private[ThriftSerialization]() extends org.apache.hadoop.io.serializer.Serializer[T] {

    var transport: Option[TIOStreamTransport] = None
    var protocol: Option[TProtocol] = None

    def open(out: OutputStream) = synchronized {
      transport = Some(new TIOStreamTransport(out))

      transport match {
        case Some(t) =>
          protocol = Some(new TBinaryProtocol(t))
        case None =>
          throw new IllegalArgumentException("Transport:TIOStreamTransport expected!")
      }
    }

    @throws[IOException]
    def serialize(t: T) {
      protocol match {
        case Some(p) =>
          try {
            t.write(p)
          } catch {
            case e: TException =>
              throw new IOException(e)
          }
        case None =>
          throw new IllegalArgumentException("Protocol not defined, please `open` the Serializer first!")
      }
    }

    @throws[IOException]
    def close() = synchronized {
      transport match {
        case Some(t) =>
          t.close()
        case None =>
      }
    }
  }

  class TDeserializerAdapter private[ThriftSerialization](tClass: Class[T]) extends org.apache.hadoop.io.serializer.Deserializer[T] {
    var transport: TIOStreamTransport = null
    var protocol: TProtocol = null

    def open(in: InputStream) {
      transport = new TIOStreamTransport(in)
      protocol = new TBinaryProtocol(transport)
    }

    private def _getThriftBase(t: T) = {
      if (t == null) {
        ReflectionUtils.newInstance(tClass, null)
      } else {
        t.clear()
        t
      }
    }

    @throws[IOException]
    def deserialize(t: T): T = {

      val _obj: T = _getThriftBase(t)

      try {
        _obj.read(protocol)
      } catch {
        case e: TException =>
          throw new IOException(e)
      }

      _obj
    }

    @throws[IOException]
    def close() = {
      if (transport != null) {
        transport.close()
      }
    }
  }

}
