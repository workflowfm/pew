package com.workflowfm.pew

import java.text.SimpleDateFormat

import com.workflowfm.pew.PiMetadata.{PiMetadataMap, SimulatedTime, SystemTime}

import scala.collection.immutable.Queue
import scala.concurrent.{ Promise, Future, ExecutionContext }

/** Super-class for any PiProcess events which take place during
  * workflow execution or simulation.
  *
  * @tparam KeyT The type used to identify PiInstances.
  */
sealed trait PiEvent[KeyT] {

  /** @return The PiInstance ID associated with this event.
    */
  def id: KeyT

  /** Holds the various measures for the time this event happened.
    */
  val metadata: PiMetadataMap

  /** @return The system time (in milliseconds) when this PiEvent
    *         actually occured during computation.
    */
  def rawTime: Long = SystemTime( metadata )

  /** @return The simulated time (in its own units) when the real-life
    *         event that is represented by this PiEvent occured.
    */
  def simTime: Long = SimulatedTime( metadata )

  def asString: String
}

/** PiEvents which are associated with a specific AtomicProcess call.
  */
sealed trait PiAtomicProcessEvent[KeyT] extends PiEvent[KeyT] {
  def ref: Int
}

sealed trait PiExceptionEvent[KeyT] extends PiEvent[KeyT] {
  def exception: PiException[KeyT]

  /** Jev, Override `toString` method as printing entire the entire trace by default gets old.
    */
  override def toString: String = {
    val ex: PiException[KeyT] = exception

    val typeName: String = ex.getClass.getSimpleName
    val message: String = ex.getMessage

    s"PiEe($typeName):$id($message)"
  }
}

case class PiEventStart[KeyT](
   i: PiInstance[KeyT],
   override val metadata: PiMetadataMap = PiMetadata()
 ) extends PiEvent[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = " === [" + i.id + "] INITIAL STATE === \n" + i.state + "\n === === === === === === === ==="
}


case class PiEventResult[KeyT](
    i: PiInstance[KeyT],
    res: Any,
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiEvent[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
      " === [" + i.id + "] RESULT: " + res
}

case class PiEventCall[KeyT](
    override val id: KeyT,
    ref: Int,
    p: MetadataAtomicProcess,
    args: Seq[PiObject],
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS CALL: ${p.name} ($ref) args: ${args.mkString(",")}"
}

case class PiEventReturn[KeyT](
    override val id: KeyT,
    ref: Int,
    result: Any,
    metadata: PiMetadataMap = PiMetadata()
  ) extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS RETURN: ($ref) returned: $result"
}

case class PiFailureNoResult[KeyT](
    i:PiInstance[KeyT],
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiEvent[KeyT] with PiExceptionEvent[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE ===\n${i.state}\n === === === === === === === ===\n === [$id] NO RESULT! ==="

  override def exception: PiException[KeyT] = NoResultException[KeyT]( i )
}

case class PiFailureUnknownProcess[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiExceptionEvent[KeyT] {

  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Unknown process: $process"

  override def exception: PiException[KeyT] = UnknownProcessException[KeyT]( i, process )
}

case class PiFailureAtomicProcessIsComposite[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiExceptionEvent[KeyT] {

  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Executor encountered composite process thread: $process"

  override def exception: PiException[KeyT] = AtomicProcessIsCompositeException[KeyT]( i, process )
}

case class PiFailureNoSuchInstance[KeyT](
    override val id: KeyT,
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiExceptionEvent[KeyT] {

	override def asString: String = s" === [$id] FAILED - Failed to find instance!"

  override def exception: PiException[KeyT] = NoSuchInstanceException[KeyT]( id )
}

case class PiEventException[KeyT](
    override val id: KeyT,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap
  ) extends PiExceptionEvent[KeyT] {

	override def asString: String = s" === [$id] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteException[KeyT]( id, message, trace, rawTime )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventException[KeyT] =>
      id == that.id && message == that.message
  }
}

case class PiEventProcessException[KeyT](
    override val id: KeyT,
    ref: Int,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap
  ) extends PiEvent[KeyT] with PiAtomicProcessEvent[KeyT] with PiExceptionEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS [$ref] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteProcessException[KeyT]( id, ref, message, trace, rawTime )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventProcessException[KeyT] =>
      id == that.id && ref == that.ref && message == that.message
  }
}

object PiEventException {
  def apply[KeyT]( id: KeyT, ex: Throwable, metadata: PiMetadataMap = PiMetadata() ): PiEventException[KeyT]
    = PiEventException[KeyT]( id, ex.getLocalizedMessage, ex.getStackTrace, metadata )
}

object PiEventProcessException {
  def apply[KeyT]( id: KeyT, ref: Int, ex: Throwable, metadata: PiMetadataMap = PiMetadata() ): PiEventProcessException[KeyT]
    = PiEventProcessException[KeyT]( id, ref, ex.getLocalizedMessage, ex.getStackTrace, metadata )
}
