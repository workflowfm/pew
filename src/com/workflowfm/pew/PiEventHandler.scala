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
  def rawTime: Long = SystemTime(metadata)

  /** @return The simulated time (in its own units) when the real-life
    *         event that is represented by this PiEvent occured.
    */
  def simTime: Long = SimulatedTime(metadata)

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
  def liftMetaFn[KeyT](fn: PiMetadataMap => PiMetadataMap): PiEvent[KeyT] => PiEvent[KeyT] = {

    // PiInstance Level PiEvents
    case e: PiEventStart[KeyT]  => e.copy(metadata = fn(e.metadata))
    case e: PiEventResult[KeyT] => e.copy(metadata = fn(e.metadata))

    // PiInstance Level PiFailures
    case e: PiFailureNoResult[KeyT]                 => e.copy(metadata = fn(e.metadata))
    case e: PiFailureUnknownProcess[KeyT]           => e.copy(metadata = fn(e.metadata))
    case e: PiFailureAtomicProcessIsComposite[KeyT] => e.copy(metadata = fn(e.metadata))
    case e: PiFailureNoSuchInstance[KeyT]           => e.copy(metadata = fn(e.metadata))
    case e: PiFailureExceptions[KeyT]               => e.copy(metadata = fn(e.metadata))

    // PiInstance-Call Level PiEvents
    case e: PiEventCall[KeyT]   => e.copy(metadata = fn(e.metadata))
    case e: PiEventReturn[KeyT] => e.copy(metadata = fn(e.metadata))

    // PiInstance-Call Level PiFailures
    case e: PiFailureAtomicProcessException[KeyT] => e.copy(metadata = fn(e.metadata))
  }
}

///////////////////////////////
// PiInstance Level PiEvents //
///////////////////////////////

case class PiEventStart[KeyT](
    i: PiInstance[KeyT],
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiEvent[KeyT] {

  override def id: KeyT         = i.id
  override def asString: String = s" === [$id] INITIAL STATE === \n${i.state}\n === === === === === === === ==="
}

/** Abstract superclass for all PiEvents which denote the termination of a PiInstance.
  */
sealed trait PiEventFinish[KeyT] extends PiEvent[KeyT]

case class PiEventResult[KeyT](
    i: PiInstance[KeyT],
    res: Any,
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiEvent[KeyT]
    with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String =
    s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
      s" === [$id] RESULT: $res"
}

////////////////////////////////////////////
// PiInstance Level Exceptions & Failures //
////////////////////////////////////////////

sealed trait PiFailure[KeyT] extends PiEvent[KeyT] {

  def exception: PiException[KeyT]

  /** Jev, Override `toString` method as printing entire the entire trace by default gets old.
    */
  override def toString: String = {
    val ex: PiException[KeyT] = exception

    val typeName: String = ex.getClass.getSimpleName
    val message: String  = ex.getMessage

    s"PiEe($typeName):$id($message)"
  }
}

object PiFailure {
  def condense[KeyT](errors: Seq[PiFailure[KeyT]]): PiFailure[KeyT] = {
    errors.head match {
      case PiFailureAtomicProcessException(id, ref, message, trace, metadata) =>
        PiFailureExceptions(id, message, trace, metadata)
      case other => other
    }
  }
}

case class PiFailureNoResult[KeyT](
    i: PiInstance[KeyT],
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiFailure[KeyT]
    with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String =
    s" === [$id] FINAL STATE ===\n${i.state}\n === === === === === === === ===\n === [$id] NO RESULT! ==="

  override def exception: PiException[KeyT] = NoResultException[KeyT](i)
}

case class PiFailureUnknownProcess[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiFailure[KeyT]
    with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String =
    s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
      s" === [$id] FAILED - Unknown process: $process"

  override def exception: PiException[KeyT] = UnknownProcessException[KeyT](i, process)
}

case class PiFailureAtomicProcessIsComposite[KeyT](
    i: PiInstance[KeyT],
    process: String,
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiFailure[KeyT]
    with PiEventFinish[KeyT] {

  override def id: KeyT = i.id
  override def asString: String =
    s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
      s" === [$id] FAILED - Executor encountered composite process thread: $process"

  override def exception: PiException[KeyT] = AtomicProcessIsCompositeException[KeyT](i, process)
}

case class PiFailureNoSuchInstance[KeyT](
    override val id: KeyT,
    override val metadata: PiMetadataMap = PiMetadata()
) extends PiFailure[KeyT]
    with PiEventFinish[KeyT] {

  override def asString: String = s" === [$id] FAILED - Failed to find instance!"

  override def exception: PiException[KeyT] = NoSuchInstanceException[KeyT](id)
}

/** At least one AtomicProcess called by this PiInstance encountered an exception.
  */
case class PiFailureExceptions[KeyT](
    override val id: KeyT,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap
) extends PiFailure[KeyT]
    with PiEventFinish[KeyT] {

  override def asString: String = s" === [$id] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteException[KeyT](id, message, trace, rawTime)

  override def equals(other: Any): Boolean = other match {
    case that: PiFailureExceptions[KeyT] =>
      id == that.id && message == that.message
  }
}

object PiFailureExceptions {
  def apply[KeyT](id: KeyT, ex: Throwable, metadata: PiMetadataMap = PiMetadata()): PiFailureExceptions[KeyT] =
    PiFailureExceptions[KeyT](id, ex.getLocalizedMessage, ex.getStackTrace, metadata)
}

///////////////////////////////////////
// AtomicProcess Call Level PiEvents //
///////////////////////////////////////

/** PiEvents which are associated with a specific AtomicProcess call.
  */
sealed trait PiAtomicProcessEvent[KeyT] extends PiEvent[KeyT] {
  def ref: Int
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

/** Abstract superclass for all PiAtomicProcessEvent which denote the termination of an AtomicProcess call.
  */
trait PiEventCallEnd[KeyT] extends PiAtomicProcessEvent[KeyT]

case class PiEventReturn[KeyT](
    override val id: KeyT,
    ref: Int,
    result: Any,
    metadata: PiMetadataMap = PiMetadata()
) extends PiAtomicProcessEvent[KeyT]
    with PiEventCallEnd[KeyT] {

  override def asString: String = s" === [$id] PROCESS RETURN: ($ref) returned: $result"
}

/////////////////////////////////////////
// AtomicProcess Call Level PiFailures //
/////////////////////////////////////////

case class PiFailureAtomicProcessException[KeyT](
    override val id: KeyT,
    ref: Int,
    message: String,
    trace: Array[StackTraceElement],
    override val metadata: PiMetadataMap
) extends PiAtomicProcessEvent[KeyT]
    with PiFailure[KeyT]
    with PiEventCallEnd[KeyT] {

  override def asString: String = s" === [$id] PROCESS [$ref] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteProcessException[KeyT](id, ref, message, trace, rawTime)

  override def equals(other: Any): Boolean = other match {
    case that: PiFailureAtomicProcessException[KeyT] =>
      id == that.id && ref == that.ref && message == that.message
  }
}

object PiFailureAtomicProcessException {
  def apply[KeyT](
      id: KeyT,
      ref: Int,
      ex: Throwable,
      metadata: PiMetadataMap = PiMetadata()
  ): PiFailureAtomicProcessException[KeyT] =
    PiFailureAtomicProcessException[KeyT](id, ref, ex.getLocalizedMessage, ex.getStackTrace, metadata)
}

////////////////////
// PiEventHanders //
////////////////////

// Return true if the handler is done and needs to be unsubscribed.

trait PiEventHandler[KeyT] extends (PiEvent[KeyT] => Boolean) {
  def name: String
  def and(h: PiEventHandler[KeyT]) = MultiPiEventHandler(this, h)
}

trait PiEventHandlerFactory[T, H <: PiEventHandler[T]] {
  def build(id: T): H
}

class PrintEventHandler[T](override val name: String) extends PiEventHandler[T] {
  val formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS")
  override def apply(e: PiEvent[T]) = {
    val time = formatter.format(e.rawTime)
    System.err.println("[" + time + "]" + e.asString)
    false
  }
}

class PromiseHandler[T](override val name: String, val id: T) extends PiEventHandler[T] {
  val promise = Promise[Any]()
  def future  = promise.future

  // class PromiseException(message:String) extends Exception(message)

  override def apply(e: PiEvent[T]) =
    if (e.id == this.id) e match {
      case PiEventResult(i, res, _) => promise.success(res); true
      case ex: PiFailure[T]         => promise.failure(ex.exception); true
      case _                        => false
    } else false
}

class PromiseHandlerFactory[T](name: T => String) extends PiEventHandlerFactory[T, PromiseHandler[T]] {
  def this(name: String) = this { _: T =>
    name
  }
  override def build(id: T) = new PromiseHandler[T](name(id), id)
}

class CounterHandler[T](override val name: String, val id: T) extends PiEventHandler[T] {
  private var counter: Int = 0
  def count                = counter
  val promise              = Promise[Int]()
  def future               = promise.future

  override def apply(e: PiEvent[T]) =
    if (e.id == this.id) e match {
      case PiEventResult(i, res, _) => counter += 1; promise.success(counter); true
      case ex: PiFailure[T]         => counter += 1; promise.success(counter); true
      case _                        => counter += 1; false
    } else false
}

class CounterHandlerFactory[T](name: T => String) extends PiEventHandlerFactory[T, CounterHandler[T]] {
  def this(name: String) = this { _: T =>
    name
  }
  override def build(id: T) = new CounterHandler[T](name(id), id)
}

case class MultiPiEventHandler[T](handlers: Queue[PiEventHandler[T]]) extends PiEventHandler[T] {
  override def name                      = handlers map (_.name) mkString (",")
  override def apply(e: PiEvent[T])      = handlers map (_(e)) forall (_ == true)
  override def and(h: PiEventHandler[T]) = MultiPiEventHandler(handlers :+ h)
}

object MultiPiEventHandler {
  def apply[T](handlers: PiEventHandler[T]*): MultiPiEventHandler[T] =
    MultiPiEventHandler[T](Queue[PiEventHandler[T]]() ++ handlers)
}

trait PiObservable[T] {
  def subscribe(handler: PiEventHandler[T]): Future[Boolean]
  def unsubscribe(handlerName: String): Future[Boolean]
}

trait SimplePiObservable[T] extends PiObservable[T] {
  import collection.mutable.Map

  implicit val executionContext: ExecutionContext

  val handlers: Map[String, PiEventHandler[T]] = Map[String, PiEventHandler[T]]()

  override def subscribe(handler: PiEventHandler[T]): Future[Boolean] = Future {
    //System.err.println("Subscribed: " + handler.name)
    handlers += (handler.name -> handler)
    true
  }

  override def unsubscribe(handlerName: String): Future[Boolean] = Future {
    handlers.remove(handlerName).isDefined
  }

  def publish(evt: PiEvent[T]) = {
    handlers.retain((k, v) => !v(evt))
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe(handler: PiEventHandler[T]): Future[Boolean] = worker.subscribe(handler)
  override def unsubscribe(handlerName: String): Future[Boolean]      = worker.unsubscribe(handlerName)
}
