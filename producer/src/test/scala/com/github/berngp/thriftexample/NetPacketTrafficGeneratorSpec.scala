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
class NetPacketTrafficGeneratorSpec extends FlatSpec with SequentialNestedSuiteExecution {

  behavior of "A Net Packet Traffic Generator"

  import NetPacketTrafficGenerator._

  object buffer {
    var builderBox: Option[NetTrafficPacketPlanBuilder[_ <: BUILDER_REQ, _ <: BUILDER_REQ]] = None
  }

  object fixtures {
    val destinationAddresses = (1 to 5).map("192.168.1." + _).map(IPv4Address(_)).map(_.get).toSet
    val sourceAddresses = (1 to 10).map("192.168.100." + _).map(IPv4Address(_)).map(_.get).toSet
    val timeSeriesSzie = 10
  }

  it should "instantiate a builder" in {
    buffer.builderBox = Some(builder())
    buffer.builderBox should be('defined)
  }

  it should "get a basic generator" in {
    val b = buffer.builderBox.get
    val g = b withDestinationsAddresses (fixtures.destinationAddresses) withSourceAddresses (fixtures.sourceAddresses) withSize (fixtures.timeSeriesSzie) build()
    val p = g.getPlan

    println(p.timeSeries)
    p.timeSeries.size should be(g.size)
  }

}
