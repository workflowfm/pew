package com.workflowfm.pew

import java.text.SimpleDateFormat

import scala.collection.immutable.Queue
import scala.concurrent.{ Promise, Future, ExecutionContext }

sealed trait PiEvent[KeyT] {
  def id:KeyT
  def asString:String
  val time:Long
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

case class PiEventStart[KeyT](i:PiInstance[KeyT], override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] INITIAL STATE === \n" + i.state + "\n === === === === === === === ==="
}


case class PiEventResult[KeyT](i:PiInstance[KeyT], res:Any, override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
      " === [" + i.id + "] RESULT: " + res
}

case class PiEventCall[KeyT](override val id:KeyT, ref:Int, p:AtomicProcess, args:Seq[PiObject], override val time:Long=System.currentTimeMillis())
  extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS CALL: ${p.name} ($ref) args: ${args.mkString(",")}"
}

case class PiEventReturn[KeyT](override val id:KeyT, ref:Int, result:Any, override val time:Long=System.currentTimeMillis())
  extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS RETURN: ($ref) returned: $result"
}

case class PiFailureNoResult[KeyT](i:PiInstance[KeyT], override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] with PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE ===\n${i.state}\n === === === === === === === ===\n === [$id] NO RESULT! ==="

  override def exception: PiException[KeyT] = NoResultException[KeyT]( i )
}

case class PiFailureUnknownProcess[KeyT](i:PiInstance[KeyT], process:String, override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Unknown process: $process"

  override def exception: PiException[KeyT] = UnknownProcessException[KeyT]( i, process )
}

case class PiFailureAtomicProcessIsComposite[KeyT]( i: PiInstance[KeyT], process: String , override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Executor encountered composite process thread: $process"

  override def exception: PiException[KeyT] = AtomicProcessIsCompositeException[KeyT]( i, process )
}

case class PiFailureNoSuchInstance[KeyT](override val id:KeyT, override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
	override def asString: String = s" === [$id] FAILED - Failed to find instance!"

  override def exception: PiException[KeyT] = NoSuchInstanceException[KeyT]( id )
}

case class PiEventException[KeyT](override val id:KeyT, message:String, trace: Array[StackTraceElement], override val time:Long) extends PiExceptionEvent[KeyT] {
	override def asString: String = s" === [$id] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteException[KeyT]( id, message, trace, time )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventException[KeyT] =>
      id == that.id && message == that.message
  }
}

case class PiEventProcessException[KeyT](override val id:KeyT, ref:Int, message:String, trace: Array[StackTraceElement], override val time:Long)
  extends PiEvent[KeyT] with PiAtomicProcessEvent[KeyT] with PiExceptionEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS [$ref] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteProcessException[KeyT]( id, ref, message, trace, time )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventProcessException[KeyT] =>
      id == that.id && ref == that.ref && message == that.message
  }
}

object PiEventException {
  def apply[KeyT]( id: KeyT, ex: Throwable, time: Long=System.currentTimeMillis() ): PiEventException[KeyT]
    = PiEventException( id, ex.getLocalizedMessage, ex.getStackTrace, time )
}

object PiEventProcessException {
  def apply[KeyT]( id: KeyT, ref: Int, ex: Throwable, time: Long=System.currentTimeMillis() ): PiEventProcessException[KeyT]
    = PiEventProcessException( id, ref, ex.getLocalizedMessage, ex.getStackTrace, time )
}

// Return true if the handler is done and needs to be unsubscribed.

trait PiEventHandler[KeyT] extends (PiEvent[KeyT]=>Boolean) {
  def name:String
  def and(h:PiEventHandler[KeyT]) = MultiPiEventHandler(this,h)
}

trait PiEventHandlerFactory[T,H <: PiEventHandler[T]] {
  def build(id:T):H
}

class PrintEventHandler[T](override val name:String) extends PiEventHandler[T] {   
  val formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS")
  override def apply(e:PiEvent[T]) = {
    val time = formatter.format(e.time)
    System.err.println("["+time+"]" + e.asString)
    false
  }
}


class PromiseHandler[T](override val name:String, val id:T) extends PiEventHandler[T] {   
  val promise = Promise[Any]()
  def future = promise.future
  
  // class PromiseException(message:String) extends Exception(message)
  
  override def apply(e:PiEvent[T]) = if (e.id == this.id) e match {  
    case PiEventResult(i,res,_) => promise.success(res); true
    case ex: PiExceptionEvent[T] => promise.failure( ex.exception ); true
    case _ => false
  } else false 
}

class PromiseHandlerFactory[T](name:T=>String) extends PiEventHandlerFactory[T,PromiseHandler[T]] {
  def this(name:String) = this { _:T => name }
  override def build(id:T) = new PromiseHandler[T](name(id),id)
}



class CounterHandler[T](override val name:String, val id:T) extends PiEventHandler[T] {   
  private var counter:Int = 0
  def count = counter
  val promise = Promise[Int]()
  def future = promise.future
  
  override def apply(e:PiEvent[T]) = if (e.id == this.id) e match {  
    case PiEventResult(i,res,_) => counter += 1 ; promise.success(counter) ; true
    case ex: PiExceptionEvent[T] => counter += 1; promise.success(counter) ; true
    case _ => counter += 1 ; false
  } else false 
}

class CounterHandlerFactory[T](name:T=>String) extends PiEventHandlerFactory[T,CounterHandler[T]] {
  def this(name:String) = this { _:T => name }
  override def build(id:T) = new CounterHandler[T](name(id),id)
}


case class MultiPiEventHandler[T](handlers:Queue[PiEventHandler[T]]) extends PiEventHandler[T] {
  override def name = handlers map (_.name) mkString(",")
  override def apply(e:PiEvent[T]) = handlers map (_(e)) forall (_ == true)
  override def and(h:PiEventHandler[T]) = MultiPiEventHandler(handlers :+ h)
}
  
object MultiPiEventHandler {
  def apply[T](handlers:PiEventHandler[T]*):MultiPiEventHandler[T] = MultiPiEventHandler[T](Queue[PiEventHandler[T]]() ++ handlers)
}


trait PiObservable[T] {
  def subscribe(handler:PiEventHandler[T]):Future[Boolean]
  def unsubscribe(handlerName:String):Future[Boolean]
}

trait SimplePiObservable[T] extends PiObservable[T] {
  import collection.mutable.Map

  implicit val executionContext:ExecutionContext

  val handlers:Map[String,PiEventHandler[T]] = Map[String,PiEventHandler[T]]()
  
  override def subscribe(handler:PiEventHandler[T]):Future[Boolean] = Future {
    System.err.println("Subscribed: " + handler.name)
    handlers += (handler.name -> handler)
    true
  }
  
  override def unsubscribe(handlerName:String):Future[Boolean] = Future {
    handlers.remove(handlerName).isDefined
  }
  
  def publish(evt:PiEvent[T]) = {
    handlers.retain((k,v) => v(evt))
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe( handler: PiEventHandler[T] ): Future[Boolean] = worker.subscribe( handler )
  override def unsubscribe( handlerName: String ): Future[Boolean] = worker.unsubscribe( handlerName )
}
