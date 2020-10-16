package com.workflowfm.pew

import scala.collection.immutable

object PiMetadata {

  /** A collection of meta-data which is associated
    * with PiEvents.
    */
  type PiMetadataMap = immutable.Map[String, Any]

  /** A tuple used to construct a meta-data map, ensures
    * the data's key type matches the data's value type.
    *
    * @tparam T The meta-data value type.
    */
  type PiMetadataElem[T] = (Key[T], T)

  /** Lightweight wrapper around a String to safely retrieve
    * a value of the given type from the metadata map.
    *
    * @param id The String key of the type of meta-data.
    * @tparam T The Scala-type of the type of meta-data.
    */
  case class Key[T](val id: String) extends (PiMetadataMap => T) {
    override def apply(meta: PiMetadataMap): T = meta(id).asInstanceOf[T]

    def get(meta: PiMetadataMap): Option[T] = meta.get(id).map(_.asInstanceOf[T])
  }

  /** The system time (in milliseconds) when a PiEvent
    * actually occured during computation.
    */
  object SystemTime extends Key[Long]("SysTime")

  /** The simulated time (in its own units) when the real-life
    * event that is represented by this PiEvent occured.
    */
  object SimulatedTime extends Key[Long]("SimTime")

  /** Construct a PiMetadataMap with the given meta-data.
    */
  def apply(data: PiMetadataElem[_]*): PiMetadataMap = {
    // Jev, ensure there's at least a SystemPiTime.
    val current = SystemTime -> System.currentTimeMillis()
    (current +: data).map(elem => (elem._1.id, elem._2)).toMap
  }

}
