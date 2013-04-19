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

/** */
class IPv4AddressSpec extends FlatSpec {


  behavior of "An IP V4 Address"

  object fixtures {
    val str = "192.168.1.1"
    val ints = str.split('.').map(_.toInt)
    val bytes = ints.map(_.toByte)
    val numValue: Long = 3232235777L
  }

  it should "build using four bytes" in {
    val b = fixtures.bytes
    check(IPv4Address(b(0), b(1), b(2), b(3)))
  }

  it should "build using four ints" in {
    val b = fixtures.ints
    check(IPv4Address(b(0), b(1), b(2), b(3)))
  }

  it should "build using ints splats" in {
    check(IPv4Address(fixtures.ints: _*))
  }

  it should "build using an array of bytes" in {
    check(IPv4Address(fixtures.bytes))
  }

  it should "build using an array of ints" in {
    check(IPv4Address(fixtures.ints))
  }

  it should "build using a string" in {
    check(IPv4Address(fixtures.str))
  }

  it should "get the Integer value of an IP" in {
    val optIp = IPv4Address(fixtures.str)
    optIp should be('defined)
    optIp.get.number should be(fixtures.numValue)
  }

  it should "build from an Integer value of an IP" in {
    val ipOpt = IPv4Address(fixtures.numValue)
    ipOpt should be('defined)
    ipOpt.get.address should be(fixtures.bytes)
  }

  def check(opt: Option[IPv4Address]) = {
    opt should be('defined)
    val ip = opt.get
    ip.address.length should be(4)
    ip.address.zipWithIndex.foreach[Unit](t => fixtures.bytes(t._2) should be(t._1))
  }
}

