package com.workflowfm.pew

import java.text.SimpleDateFormat

import com.workflowfm.pew.PiMetadata.{PiMetadataMap, SimulatedTime, SystemTime}

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}


///////////////////////
// Abstract PiEvents //
///////////////////////

// TODO: Jev: Should this be moved into it's own file now?

/** Super-class for any PiProcess events which take place during
  * workflow execution or simulation. These can be understood as
  * being divided in 2 ways (into 4 categories):
  *
  * By Level:
  *   - Workflow Level: for events concerning CompositeProcess
  *   begun with a call to `ProcessExecutor.execute`. These are
  *   keyed by at least the ID of the corresponding PiInstance.
  *   - Atomic Process Level: for events concerning AtomicProcess
  *   that were called by an ProcessExecutor during the execution
  *   of a Workflow Level CompositeProcess. These are keyed by
  *   both the PiInstance ID and the individual call ID (which
  *   uniquely enumerates each call made by a PiInstance).
  *
  * By Type:
  *   - Start: these events mark the start of process execution.
  *   - End: these events mark the termination of process
  *   execution, either by successful completion along with a
  *   result or by a failure. These should always be preceded by
  *   a corresponding start/call event.
  *
  * @tparam KeyT The type used to identify PiInstances.
  */
sealed trait PiEvent[KeyT] {

  /** Retrieve the unique PiInstance ID associated with this event.
    *
    * @return A unique ID of type `KeyT`.
    */
  def id: KeyT

  /** Holds the various measures for the time this event happened.
    */
  val metadata: PiMetadataMap

  /** @return The system time (in milliseconds) when this PiEvent
    *         actually occurred during computation.
    */
  def rawTime: Long = SystemTime( metadata )

  /** @return The simulated time (in its own units) when the real-life
    *         event that is represented by this PiEvent occured.
    */
  def simTime: Long = SimulatedTime( metadata )

  def asString: String
}

object PiEvent {

  /** Update the metadata of this event whilst copying the rest of the
    * members of this event. Provides a way of modifying the PiEvent
    * metadata since otherwise that information is immutable.
    *
    * @param fn The function to use to update the metadata.
    * @tparam KeyT The type used to identify PiInstances.
    * @return A function to which returns events with modified metadata.
    */
  def liftMetaFn[KeyT]( fn: PiMetadataMap => PiMetadataMap )
    : PiEvent[KeyT] => PiEvent[KeyT] = {

    // PiInstance Level PiEvents
    case e: PiEventStart[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiEventResult[KeyT] => e.copy( metadata = fn( e.metadata ) )

    // PiInstance Level PiFailures
    case e: PiFailureNoResult[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiFailureUnknownProcess[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiFailureAtomicProcessIsComposite[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiFailureNoSuchInstance[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiFailureExceptions[KeyT] => e.copy( metadata = fn( e.metadata ) )

    // PiInstance-Call Level PiEvents
    case e: PiEventCall[KeyT] => e.copy( metadata = fn( e.metadata ) )
    case e: PiEventReturn[KeyT] => e.copy( metadata = fn( e.metadata ) )

    // PiInstance-Call Level PiFailures
    case e: PiFailureAtomicProcessException[KeyT] => e.copy( metadata = fn( e.metadata ) )
  }
}


///////////////////////////////
// PiInstance Level PiEvents //
///////////////////////////////

/** Denotes the start of execution of a CompositeProcess.
  *
  * @param i PiInstance representing the start state of this PiProcess.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiEventStart[KeyT](
   i: PiInstance[KeyT],
   override val metadata: PiMetadataMap = PiMetadata()
 ) extends PiEvent[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] INITIAL STATE === \n${i.state}\n === === === === === === === ==="
}

/** Abstract superclass for all PiEvents which denote the termination of a PiInstance.
  */
sealed trait PiEventFinish[KeyT] extends PiEvent[KeyT]

/** Denotes the successful completion of execution of a CompositeProcess.
  *
  * @param i PiInstance representing the terminal state of this PiProcess.
  * @param res The corresponding `result` object of this CompositeProcess.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiEventResult[KeyT](
    i: PiInstance[KeyT],
    res: Any,
    override val metadata: PiMetadataMap = PiMetadata()

  ) extends PiEvent[KeyT] with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
    s" === [$id] RESULT: $res"
}


////////////////////////////////////////////
// PiInstance Level Exceptions & Failures //
////////////////////////////////////////////

/** Denotes that the corresponding PiInstance failed to complete execution.
  *
  * @tparam KeyT The type used to identify PiInstances.
  */
sealed trait PiFailure[KeyT]
  extends PiEvent[KeyT] {

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

object PiFailure {

  /** Helper function for ProcessExecutors providing standard functionality
    * for upgrading AtomicProcess failures into a single error suitable for
    * terminating a CompositeProcess.
    *
    * @param errors A list of AP failures encountered in a PiInstance.
    * @tparam KeyT The type used to identify PiInstances.
    * @return A single error to throw and terminate the PiInstance execution.
    */
  def condense[KeyT]( errors: Seq[PiFailure[KeyT]] ): PiFailure[KeyT] = {
    errors.head match {
      case PiFailureAtomicProcessException(id, ref, message, trace, metadata) =>
        PiFailureExceptions(id, message, trace, metadata)
      case other => other
    }
  }
}

/** The ProcessExecutor could not provide a `result` after completing
  * execution of this PiInstance.
  *
  * @param i PiInstance representing the final state of this PiProcess.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiFailureNoResult[KeyT](
    i: PiInstance[KeyT],
    override val metadata: PiMetadataMap = PiMetadata()

  ) extends PiFailure[KeyT] with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE ===\n${i.state}\n === === === === === === === ===\n === [$id] NO RESULT! ==="

  override def exception: PiException[KeyT] = NoResultException[KeyT]( i )
}

/** The ProcessExecutor could not complete execution of this PiInstance
  * because it required execution of an AtomicProcess unknown to the
  * ProcessExecutor.
  *
  * (Note: This indicates the ProcessExecutor has been incorrectly
  * configured)
  *
  * @param i PiInstance representing the final state of this PiProcess.
  * @param process Name of the process that could not be identified.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiFailureUnknownProcess[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()

  ) extends PiFailure[KeyT] with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
    s" === [$id] FAILED - Unknown process: $process"

  override def exception: PiException[KeyT] = UnknownProcessException[KeyT]( i, process )
}

/** The ProcessExecutor attempted to dispatch a CompositeProcess to the
  * AtomicProcessExecutor, this is likely because it was unable to be
  * fully reduced.
  *
  * (Note: This indicates the workflow has not been properly constructed)
  *
  * @param i PiInstance representing the final state of this PiProcess.
  * @param process Name of the unexpected CompositeProcess.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiFailureAtomicProcessIsComposite[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()

  ) extends PiFailure[KeyT] with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
    s" === [$id] FAILED - Executor encountered composite process thread: $process"

  override def exception: PiException[KeyT] = AtomicProcessIsCompositeException[KeyT]( i, process )
}

/** A ProcessExecutor could not initiate execution of this PiInstance
  * because it did not have a record of a prior initialisation using
  * `ProcessExecutor.init`.
  *
  * @param id Unrecognised PiInstance ID attempting execution.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
// TODO: Jev, should this actually be marked `PiEventFinish`.
case class PiFailureNoSuchInstance[KeyT](
    override val id: KeyT,
    override val metadata: PiMetadataMap = PiMetadata()

  ) extends PiFailure[KeyT] with PiEventFinish[KeyT] {

  override def asString: String = s" === [$id] FAILED - Failed to find instance!"

  override def exception: PiException[KeyT] = NoSuchInstanceException[KeyT]( id )
}

/** At least one AtomicProcess called by this PiInstance encountered
  * irrecoverable exceptions during execution.
  *
  * @param id ID of corresponding PiInstance.
  * @param message Message of Exception encountered by the AtomicProcess.
  * @param trace A full trace of the encountered Exception.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiFailureExceptions[KeyT](
    override val id: KeyT,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap

  ) extends PiFailure[KeyT] with PiEventFinish[KeyT] {

  override def asString: String = s" === [$id] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteException[KeyT]( id, message, trace, rawTime )

  override def equals( other: Any ): Boolean = other match {
    case that: PiFailureExceptions[KeyT] =>
      id == that.id && message == that.message
  }
}

object PiFailureExceptions {
  def apply[KeyT]( id: KeyT, ex: Throwable, metadata: PiMetadataMap = PiMetadata() ): PiFailureExceptions[KeyT]
    = PiFailureExceptions[KeyT]( id, ex.getLocalizedMessage, ex.getStackTrace, metadata )
}


///////////////////////////////////////
// AtomicProcess Call Level PiEvents //
///////////////////////////////////////

/** PiEvents which are associated with a specific AtomicProcess call.
  *
  * @tparam KeyT The type used to identify PiInstances.
  */
sealed trait PiAtomicProcessEvent[KeyT] extends PiEvent[KeyT] {
  def ref: Int
}

/** Denotes the start of a AtomicProcess execution made during the execution
  * of a parent CompositeProcess.
  *
  * @param id PiInstance ID for the parent CompositeProcess.
  * @param ref Enumerated ID for this AtomicProcess call, unique for this PiInstance.
  * @param p Reference to AtomicProcess called.
  * @param args Parameter values for the AtomicProcess call.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiEventCall[KeyT](
    override val id: KeyT,
    ref: Int,
    p: MetadataAtomicProcess,
    args: Seq[PiObject],
    override val metadata: PiMetadataMap = PiMetadata()
  ) extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS CALL: ${p.name} ($ref) args: ${args.mkString(",")}"
}

/** Abstract superclass for all PiAtomicProcessEvent which denote the termination
  * of an AtomicProcess call.
  *
  * @tparam KeyT The type used to identify PiInstances.
  */
trait PiEventCallEnd[KeyT] extends PiAtomicProcessEvent[KeyT]

/** Denotes the successful completion of execution of a AtomicProcess.
  *
  * @param id PiInstance ID for the parent CompositeProcess.
  * @param ref Enumerated ID for this AtomicProcess call, unique for this PiInstance.
  * @param result Object returned by this execution of the AtomicProcess.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiEventReturn[KeyT](
    override val id: KeyT,
    ref: Int,
    result: Any,
    metadata: PiMetadataMap = PiMetadata()

  ) extends PiAtomicProcessEvent[KeyT] with PiEventCallEnd[KeyT] {

	override def asString: String = s" === [$id] PROCESS RETURN: ($ref) returned: $result"
}


/////////////////////////////////////////
// AtomicProcess Call Level PiFailures //
/////////////////////////////////////////

/** Denotes that a AtomicProcess call was terminated by encountering an Exception
  * from which it could not recover.
  *
  * @param id PiInstance ID for the parent CompositeProcess.
  * @param ref Enumerated ID for this AtomicProcess call, unique for this PiInstance.
  * @param message Message of the Exception encountered.
  * @param trace A full trace of the encountered Exception.
  * @param metadata Metadata object.
  * @tparam KeyT The type used to identify PiInstances.
  */
case class PiFailureAtomicProcessException[KeyT](
    override val id: KeyT,
    ref: Int,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap

  ) extends PiAtomicProcessEvent[KeyT] with PiFailure[KeyT] with PiEventCallEnd[KeyT] {

	override def asString: String = s" === [$id] PROCESS [$ref] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteProcessException[KeyT]( id, ref, message, trace, rawTime )

  /** @param other Object to compare to this.
    * @return True if both objects concern the same failure.
    */
  override def equals( other: Any ): Boolean = other match {
    case that: PiFailureAtomicProcessException[KeyT] =>
      id == that.id && ref == that.ref && message == that.message

    case _ => false
  }
}

object PiFailureAtomicProcessException {
  def apply[KeyT]( id: KeyT, ref: Int, ex: Throwable, metadata: PiMetadataMap = PiMetadata() ): PiFailureAtomicProcessException[KeyT]
    = PiFailureAtomicProcessException[KeyT]( id, ref, ex.getLocalizedMessage, ex.getStackTrace, metadata )
}
