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
import net.liftweb.common.Logger
import org.apache.hadoop.io.serializer.{Serializer, Deserializer}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.thrift._
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory, TProtocol}
import org.apache.thrift.transport.{TTransport, TIOStreamTransport}


abstract class ThriftWritableAdapter[T <: TBase[TBase[_, _], _ <: TFieldIdEnum]](bytes: Array[Byte], length: Int)
  extends org.apache.hadoop.io.BytesWritable(bytes, length)
  with Logger {

  private var base: Option[T] = None

  def this(b: Array[Byte]) = this(b, b.length)

  def this() = this(Array.empty[Byte])

  def this(thrift: T, b: Array[Byte], l: Int) = {
    this(b, l)
    base = Some(thrift)
  }

  def newBaseInstance: T

  val baseClass: Class[T]

  private val factory: TProtocolFactory = new TBinaryProtocol.Factory()

  /** Sets the Max Length of bytes allocated for serializing the Thrift Object, please overload if the object you expect is larger.
    * Current Max Value is **1073741824**.
    * */
  protected def getMaxLength = 1073741824

  def _ensureBaseInstance = synchronized {
    base match {
      case None =>
        base = Some(newBaseInstance)
      case _ =>
    }
    base
  }

  private def toBytes = _ensureBaseInstance match {
    case Some(t) =>
      val ser = new TSerializer(factory)
      val bytes = ser.serialize(t)
      bytes
    case None =>
      Array.empty[Byte]
  }

  @throws[IOException]
  def getFromBytes: T = _ensureBaseInstance match {
    case Some(t) =>
      readFields(new DataInputStream(new ByteArrayInputStream(this.bytes)))
      t
    case None =>
      throw new IllegalStateException("Unable to ensure a base reference.")
  }

  @throws[IOException]
  override def write(out: DataOutput) {
    val bytes = toBytes
    require(bytes.length < getMaxLength,
      s"Length of the writable ${bytes.length} exceeds the max allowed of ${getMaxLength} bytes, if intended please override `getMaxLength`.")

    if (bytes.length > 0) {
      out.writeInt(bytes.length)
      out.write(bytes, 0, bytes.length)
    } else {
      out.write(0)
    }
  }

  @Override
  @throws[IOException]
  override def readFields(in: DataInput) = _ensureBaseInstance match {
    case Some(t) =>
      val length = in.readInt()
      require(length < getMaxLength,
        s"Length of the writable [${length}}] exceeds the max allowed of ${getMaxLength} bytes, if intended please override `getMaxLength`.")
      val buff = new Array[Byte](length)

      in.readFully(buff, 0, length)
      val dser = new TDeserializer(factory)

      _ensureBaseInstance
      dser.deserialize(t, buff)
    case None =>
      throw new IllegalStateException("Unable to ensure a base reference.")
  }
}


trait ThriftHadoopWritable[T <: TBase[_, _], F <: TFieldIdEnum]
  extends TBase[T, F]
  with org.apache.hadoop.io.Writable
  with Logger {

  private val factory: TProtocolFactory = new TBinaryProtocol.Factory()

  /** Sets the Max Length of bytes allocated for serializing the Thrift Object, please overload if the object you expect is larger.
    * Current Max Value is **1073741824**.
    * */
  protected def getMaxLength = 1073741824

  @throws[IOException]
  override def write(out: DataOutput) {
    val ser = new TSerializer(factory)
    val bytes = ser.serialize(this)
    require(bytes.length < getMaxLength,
      s"Length of the writable ${bytes.length} exceeds the max allowed of ${getMaxLength} bytes, if intended please override `getMaxLength`.")

    if (bytes.length > 0) {
      out.writeInt(bytes.length)
      out.write(bytes, 0, bytes.length)
    } else {
      out.write(0)
    }
  }

  @Override
  @throws[IOException]
  override def readFields(in: DataInput) {
    val length = in.readInt()
    require(length < getMaxLength,
      s"Length of the writable [${length}}] exceeds the max allowed of ${getMaxLength} bytes, if intended please override `getMaxLength`.")
    val buff = new Array[Byte](length)

    in.readFully(buff, 0, length)
    val dser = new TDeserializer(factory)

    dser.deserialize(this, buff)
  }

}

/**
 *
 * TODO Refacotr, avoid duplication on the _*Serializers_
 */
class ThriftSerialization[T <: TBase[T, _]]
  extends org.apache.hadoop.io.serializer.Serialization[T] with Logger {

  def accept(c: Class[_]): Boolean = {
    (c: @unchecked).getInterfaces.contains(classOf[TBase[_, _]])
  }

  private def getProtocolFactory = new TBinaryProtocol.Factory()

  def getSerializer(c: Class[T]): Serializer[T] = new TSerializerAdapter

  class TSerializerAdapter extends org.apache.hadoop.io.serializer.Serializer[T] {

    protected val factory: TProtocolFactory = getProtocolFactory

    private def getOutputTransport(out: OutputStream): TIOStreamTransport = {
      new TIOStreamTransport(out)
    }

    private def getOutputProtocol(transport: TTransport): TProtocol = {
      factory getProtocol transport
    }

    private var _transport: TTransport = null

    private var _protocol: TProtocol = null

    private var _out: OutputStream = null

    def open(out: OutputStream) = synchronized {
      require(out != null, "OutputStream required!")
      _out = out
      _transport = getOutputTransport(_out)
      _protocol = getOutputProtocol(_transport)
    }

    @throws[IOException]
    def serialize(t: T) {
      require(_protocol != null, "A Transport Protocol is missing, please open the Serializer!")
      try {
        t.write(_protocol)
      } catch {
        case e: TException =>
          throw new IOException(e)
      }
    }

    //TODO Refactor, clean duplication and consider using tryo
    @throws[IOException]
    def close() = synchronized {
      if (_transport != null) {
        try {
          _transport.close()
        } catch {
          case t: Throwable =>
            warn("Throwable caught while closing transport.", t)
        }
      }
      if (_out != null) {
        try {
          _out.close()
        } catch {
          case t: Throwable =>
            warn("Throwable caught while closing Output Stream.", t)
        }
      }
    }
  }

  def getDeserializer(c: Class[T]): Deserializer[T] = new TDeserializerAdapter(c)

  class TDeserializerAdapter(tClass: Class[T]) extends org.apache.hadoop.io.serializer.Deserializer[T] {

    protected val factory: TProtocolFactory = getProtocolFactory

    private def getInputTransport(in: InputStream): TIOStreamTransport = {
      new TIOStreamTransport(in)
    }

    private def getInputProtocol(transport: TTransport): TProtocol = {
      factory getProtocol transport
    }

    private var _transport: TTransport = null

    private var _protocol: TProtocol = null

    private var _in: InputStream = null

    def open(in: InputStream) = synchronized {
      _in = in
      _transport = getInputTransport(_in)
      _protocol = getInputProtocol(_transport)
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
      require(_protocol != null, "A Transport Protocol is missing, please open the Deserializer!")
      val base = _getThriftBase(t)
      base.read(_protocol)
      base
    }

    //TODO Refactor, clean duplication and consider using tryo
    @throws[IOException]
    def close() = synchronized {
      if (_transport != null) {
        try {
          _transport.close()
        } catch {
          case t: Throwable =>
            warn("Throwable caught while closing transport.", t)
        }
      }
      if (_in != null) {
        try {
          _in.close()
        } catch {
          case t: Throwable =>
            warn("Throwable caught while closing InputStream.", t)
        }
      }
    }
  }

}
