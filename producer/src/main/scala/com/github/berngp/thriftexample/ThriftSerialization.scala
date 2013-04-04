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

import java.io._
import org.apache.hadoop.io.serializer.{Serializer, Deserializer}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.thrift._
import org.apache.thrift.protocol.{TCompactProtocol, TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.TIOStreamTransport
import collection.mutable.ArrayBuffer
import net.liftweb.common.Logger


trait ThriftHDFSWritable[T <: TBase[_, _], F <: TFieldIdEnum]
  extends TBase[T, F]
  with org.apache.hadoop.io.Writable
  with Logger {

  @throws[IOException]
  def write(out: DataOutput) = {
    error("Writting from " + this.getClass)
    val ser = new TSerializer(new TBinaryProtocol.Factory())
    out.write(ser.serialize(this))
    error("Written.")
  }

  @throws[IOException]
  def readFields(in: DataInput) {
    val buf: ArrayBuffer[Byte] = ArrayBuffer()
    try {
      buf += in.readByte()
    } catch {
      case e: EOFException =>
        val des = new TDeserializer(new TCompactProtocol.Factory())
        des.deserialize(this, buf.toArray)
    }
  }
}

/** */
class ThriftSerialization[T <: TBase[T, _]]
  extends org.apache.hadoop.io.serializer.Serialization[T] with Logger {

  def accept(c: Class[_]): Boolean = {
    debug("Accepting " + c + "?")
    throw new IllegalStateException("Accepting " + c + "?")
    (c: @unchecked).isInstanceOf[TBase[_, _]]
  }

  def getSerializer(c: Class[T]): Serializer[T] = new TSerializerAdapter

  def getDeserializer(c: Class[T]): Deserializer[T] = new TDeserializerAdapter(c)

  class TSerializerAdapter extends org.apache.hadoop.io.serializer.Serializer[T] {
    var transport: Option[TIOStreamTransport] = None
    var protocol: Option[TProtocol] = None

    def open(out: OutputStream) = synchronized {
      error("Opening ThriftSerialization")
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
      error("Serializing using " + this.getClass)
      throw new IllegalStateException("Serializing using " + this.getClass)
      protocol match {
        case Some(p) =>
          try {
            t.write(p)
          } catch {
            case e: TException =>
              throw new IOException(e)
          }
        case None =>
          throw new IllegalStateException("Protocol not defined, please `open` the Serializer first!")
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

  class TDeserializerAdapter(tClass: Class[T]) extends org.apache.hadoop.io.serializer.Deserializer[T] {
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
