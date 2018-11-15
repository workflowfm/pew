package com.workflowfm.pew

import scala.collection.immutable

trait PiTimeType

/** The system time (in milliseconds) when this PiEvent
  * actually occured during computation.
  */
object SystemPiTime extends PiTimeType {
  override def toString: String = "SystemTimeMs"
}

/** The simulated time (in its own units) when the real-life
  * event that is represented by this PiEvent occured.
  */
object SimulatedPiTime extends PiTimeType {
  override def toString: String = "SimulatedTime"
}

/** A collection of times for different purposes.
  *
  * @param times A map of time values index by interpretation.
  */
class PiTimes( times: immutable.Map[PiTimeType, Long] ) {
  def apply( piType: PiTimeType ): Long = times( piType )
  def to: Seq[(PiTimeType, Long)] = times.to
}

object PiTimes {
  def apply( piTimes: (PiTimeType, Long)* ): PiTimes = {
    // Jev, ensure there's at least a SystemPiTime.
    lazy val current = SystemPiTime -> System.currentTimeMillis()
    new PiTimes( (current +: piTimes).toMap )
  }
}