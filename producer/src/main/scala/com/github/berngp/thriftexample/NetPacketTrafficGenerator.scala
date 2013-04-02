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

import collection.immutable.TreeSet
import util.Random

/** */
object NetPacketTrafficGenerator {

  object Voting {

    trait Strategy {
      def vote(candidate: IPv4Address): Boolean

      def ~(candidate: IPv4Address): Boolean = vote(candidate)
    }

    class Constant(flag: Boolean = true) extends Strategy {
      def vote(c: IPv4Address): Boolean = flag
    }

    def constant(flag: Boolean = true) = new Constant(flag)

    class Guassian(varianceTable: Map[IPv4Address, Double]) extends Strategy {
      def vote(candidate: IPv4Address) = false
    }

    def gaussian(v: Map[IPv4Address, Double]) = new Guassian(v)

    class Random extends Strategy {
      def vote(candidate: IPv4Address) = Random.nextBoolean()
    }

    def random() = new Random()

  }

  object BytePacketSize {

    trait Strategy {
      def size(candidate: Long): Int

      def ~(candidate: Long) = size(candidate)
    }

    class Random extends Strategy {
      def size(candidate: Long) = Math.abs(Random.nextInt())
    }

    def random() = new Random()

    class Constant(size: Int = Math.abs(Random.nextInt())) extends Strategy {
      def size(candidate: Long) = size
    }

    def constant(size: Int) = new Constant(size)

  }

  case class NetPacketTrafficPlan private[NetPacketTrafficGenerator](histogram: TreeSet[HistogramUnit], timeSeries: Seq[NetBinPacket])

  case class HistogramUnit private[NetPacketTrafficGenerator](destinationIp: Long, totalBytes: Int = 0) extends Ordered[HistogramUnit] {
    /** Compares two Histogram units such that the Units with the largest _totalBytes_ will be closer to the head of the Set. */
    def compare(that: HistogramUnit): Int = that.totalBytes - this.totalBytes
  }

  /**
   *
   * @param destinationAddressSet
   * @param sourceAddressSet
   * @param voter
   * @param byteSize
   * @param size
   */
  case class NetPacketTrafficPlanRecipe private[NetPacketTrafficGenerator](destinationAddressSet: Set[IPv4Address],
                                                                           sourceAddressSet: Set[IPv4Address],
                                                                           voter: Voting.Strategy,
                                                                           byteSize: BytePacketSize.Strategy,
                                                                           size: Int) {

    require(!(destinationAddressSet == null || destinationAddressSet.isEmpty), "Please provide a non-empty _Destination Address Set_")
    require(!(sourceAddressSet == null || sourceAddressSet.isEmpty), "Please provide a non-empty _Source Address Set_ ")
    require(voter != null, "A voting strategy is expected.")
    require(voter != null, "A byte size strategy is expected.")
    require(size >= 0, "The size:Int should be a positive value")


    def getPlan: NetPacketTrafficPlan = {
      val timeSeries = _getTimeSeries(size)
      val histogram = _getHistogram(timeSeries)
      NetPacketTrafficPlan(histogram, timeSeries)

    }

    /**
     *
     * @param size
     * @return
     */
    private[thriftexample] def _getTimeSeries(size: Int) = (1 to size).toSeq.map(_generateNetBin(_))

    /**
     *
     * @param sequenceId
     * @param sourceId
     * @return
     */
    private[thriftexample] def _generateNetBin(sequenceId: Int, sourceId: Int = 0) = {

      // just build a header
      val header = NetPacketHeader(
        version = 1,
        sysUpTime = System.currentTimeMillis(),
        unixSecs = (System.currentTimeMillis() / 1000).toInt,
        sequenceNumber = sequenceId,
        sourceId = sourceId
      )

      // Create a structure that will randomly gives us an element of this collection.
      val randomSources = Randomizer(sourceAddressSet)
      // Define the template of the NetRecord as a partial function
      // where the flowSetId = 0, templateId = 1, ipV4SrcAdd is selected randomly, ipv4NextHop = 1, inPkts is a random number between 1 and 255
      def _nF: (Long,Int) => NetRecord = new NetRecord(0, 1, _, randomSources.next.number, 1, (Random.nextInt(255) + 1).toShort, _)
      // Select which destination addresses will get pinged by the source addresses, then transform those into a NetRecord flow
      val netRecords = destinationAddressSet.filter(voter ~ _).map(d => _nF(d.number, (byteSize ~ d.number))).toList
      // all set, return the bin.
      new NetBinPacket(header, netRecords)
    }

    /**
     *
     * @param timeSeries
     * @return
     */
    private[thriftexample] def _getHistogram(timeSeries: Seq[NetBinPacket]) = {
      val seqOp =
        (m: Map[Long, Int], r: NetRecord) =>
          m + (r.ipV4DstAddr -> (m.getOrElse(r.ipV4DstAddr, 0) + r.inBytes))

      val combOp =
        (map1: Map[Long, Int], map2: Map[Long, Int]) =>
          (map1.keySet ++ map2.keySet).foldLeft(Map[Long, Int]()) {
            (result, k) =>
              result + (k -> (map1.getOrElse(k, 0) + map2.getOrElse(k, 0)))
          }

      val histogram =
        timeSeries.map(_.records).flatten.
          par.aggregate(Map[Long, Int]())(seqOp, combOp).
          toSeq.map(t => HistogramUnit(t._1, t._2))

      new TreeSet[HistogramUnit]() ++ histogram

    }

  }


  /**
   * Type used to mark an attribute of the builder as required.
   * TODO Document how this works to ensure all required attributes are set before we generate a writer.
   */
  abstract class BUILDER_REQ

  /** */
  abstract class PRESENT extends BUILDER_REQ

  /** */
  abstract class MISSING extends BUILDER_REQ

  class NetTrafficPacketPlanBuilder[DA <: BUILDER_REQ, SA <: BUILDER_REQ] private[NetPacketTrafficGenerator](val destinationAddressSet: Set[IPv4Address] = Set.empty,
                                                                                                             val sourceAddressSet: Set[IPv4Address] = Set.empty,
                                                                                                             val voter: Option[Voting.Strategy] = Some(Voting.random()),
                                                                                                             val byteSize: Option[BytePacketSize.Strategy] = Some(BytePacketSize.constant(256)),
                                                                                                             val size: Option[Int] = Some(100)) {

    private def _builder[DA <: BUILDER_REQ, SA <: BUILDER_REQ] = {
      new NetTrafficPacketPlanBuilder[DA, SA](_, _, _, _, _)
    }

    def withDestinationsAddresses(d: Set[IPv4Address]) = {
      require(d.nonEmpty, "Destination Addresses Expected!")
      _builder[PRESENT, SA](d, sourceAddressSet, voter, byteSize, size)
    }

    def withSourceAddresses(s : Set[IPv4Address]) = {
      require(s.nonEmpty, "Source Addresses expected!")
      _builder[DA, PRESENT](destinationAddressSet, s, voter, byteSize, size)
    }

    def withVoter(v: Voting.Strategy) = {
      require(v != null, "Expected a voter but got null!")
      _builder[DA, SA](destinationAddressSet, sourceAddressSet, Some(v), byteSize, size)
    }

    def withByteSizeStrategy(b: BytePacketSize.Strategy) = {
      require(b != null, "Expected Byte Strategy Strategy but got null!")
      _builder[DA, SA](destinationAddressSet, sourceAddressSet, voter, Some(b), size)
    }

    def withSize(u: Int) = {
      require(u > 0, "Expected Size to be greater than 0!")
      _builder[DA, SA](destinationAddressSet, sourceAddressSet, voter, byteSize, Some(u))
    }

  }


  implicit def enableBuild(b: NetTrafficPacketPlanBuilder[PRESENT, PRESENT]) = new {
    def build() = {
      new NetPacketTrafficPlanRecipe(b.destinationAddressSet, b.sourceAddressSet, b.voter.get, b.byteSize.get, b.size.get)
    }
  }

  def builder() =
    new NetTrafficPacketPlanBuilder[MISSING, MISSING]()

  /**
   * TODO add to a common lib folder.
   * @param set
   */
  private case class Randomizer[A : Manifest](set: Set[A]) {
    private val _array:Array[A] = set.toArray

    def next = _array(Random.nextInt(_array.size))
  }

}


