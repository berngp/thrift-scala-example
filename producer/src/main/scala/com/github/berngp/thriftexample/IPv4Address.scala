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

import java.net.InetAddress
import scala.language.implicitConversions

class IPv4Address private(o1: Byte, o2: Byte, o3: Byte, o4: Byte) {


  /** Array representation of the four binary octets that represent this _IP V4 Address_
    * e.g. address "127.0.0.1" would be `Array[Byte](127,0,0,1)`
    */
  lazy val address: Array[Byte] = Array(o1, o2, o3, o4)

  lazy val inet = InetAddress.getByAddress(address)

  /**
   * @return Numeric representation of this IPv4 Address.
   */
  lazy val number: Long =
    address.reverse.map(b => b.toLong & 0xFFL).zipWithIndex.map(t => (t._1 << (t._2 * 8L))).reduce(_ | _)


  /** Return a printable version of this IP address.
    *
    * @return the printable version
    */
  override val toString: String = inet.toString


  /** Overloaded "==" method to test for equality.
    *
    * @param other  object against which to test this object
    * @return `true` if equal, `false` if not
    */
  override def equals(other: Any): Boolean =
    other match {
      case that: IPv4Address =>
        that.address == this.address
      case _ =>
        false
    }

  /** Overloaded hash method: Ensures that two `IPv4Address` objects
    * that represent the same IP address have the same hash code.
    *
    * @return the hash code
    */
  override def hashCode: Int = address.hashCode
}

/**
 * @todo change the apply methods to return Option[IPv4Address]
 */
object IPv4Address {

  lazy val MAX_IPV_NUMERIC_VALUE = "256.256.256.256".
    split('.').reverse.map(_.toLong).zipWithIndex.map(t => (t._1 << (t._2 * 8L))).reduce(_ | _)

  /** Singleton `IPv4Address` for the local loop address. */
  val LOCALHOST = IPv4Address(127, 0, 0, 1)


  private def validateOctet(b: Byte) = {
    (b & 0xFF) >= 0x00 && (b & 0xFF) < 0xFF
  }

  def apply(o1: Byte, o2: Byte, o3: Byte, o4: Byte): Option[IPv4Address] = {
    Array(o1, o2, o3, o4).filterNot(validateOctet(_)) match {
      case Array() =>
        Some(new IPv4Address(o1, o2, o3, o4))
      case _ =>
        None
    }
  }

  def apply(address: Array[Byte]): Option[IPv4Address] = {
    if (address.length == 4)
      apply(address(0), address(1), address(2), address(3))
    else
      None
  }

  /** */
  def apply(ip: Long): Option[IPv4Address] = {
    ip match {
      case n if (n > 0 && n < MAX_IPV_NUMERIC_VALUE) =>
        Some(new IPv4Address(_octet(ip)(24), _octet(ip)(16), _octet(ip)(8), _octet(ip)(0)))
      case _ =>
        None
    }
  }

  private def _octet(ip: Long)(o: Byte): Byte =
    ((ip >> o) & 0xFF).toByte

  def apply(address: List[Byte]): Option[IPv4Address] =
    apply(address.toArray)

  def apply(address: Array[Int]): Option[IPv4Address] =
    apply(address.map(_.toByte))

  def apply(address: Int*): Option[IPv4Address] =
    apply(address.map(_.toByte).toArray)

  /** Create an `IPv4Address`, given a host name.
    *
    * @param host  the host name
    *
    * @return the corresponding `IPv4Address` object.
    *
    * @throws java.net.UnknownHostException unknown host
    */
  def apply(host: String): Option[IPv4Address] =
    apply(InetAddress.getByName(host).getAddress)


  /** Implicitly converts a `java.net.InetAddress` to an `IPv4Address`.
    *
    * @param address  the `java.net.InetAddress`
    * @return the corresponding `IPv4Address`
    */
  implicit def inetToIPv4Address(address: InetAddress): IPv4Address = {
    val a = address.getAddress
    new IPv4Address(a(0), a(1), a(2), a(3))
  }

  /** Implicitly converts an `IPv4Address` to a `java.net.InetAddress`.
    *
    * @param address  the `IPv4Address`
    * @return the corresponding `java.net.InetAddress`
    */
  implicit def ipToInetAddress(address: IPv4Address): InetAddress =
    InetAddress.getByAddress(address.address)

}
