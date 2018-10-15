package com.workflowfm.pew

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.immutable.Queue
import scala.concurrent.Promise

sealed trait PiEvent[KeyT] {
  def id:KeyT
  def asString:String
}

case class PiEventStart[KeyT](i:PiInstance[KeyT]) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] INITIAL STATE === \n" + i.state + "\n === === === === === === === ==="
}
case class PiEventResult[KeyT](i:PiInstance[KeyT], res:Any) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
      " === [" + i.id + "] RESULT: " + res
}
case class PiEventCall[KeyT](override val id:KeyT, ref:Int, p:AtomicProcess, args:Seq[PiObject]) extends PiEvent[KeyT] {
	override def asString = " === [" + id + "] PROCESS CALL:" +  p.name + " (" + ref + ") args: " + args.mkString(",")
}
case class PiEventReturn[KeyT](override val id:KeyT, ref:Int, result:Any) extends PiEvent[KeyT] {
	override def asString = " === [" + id + "] PROCESS RETURN: (" + ref + ") returned: " + result  
}
case class PiFailureNoResult[KeyT](i:PiInstance[KeyT]) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
		  " === [" + i.id + "] NO RESULT! ==="
}
case class PiFailureUnknownProcess[KeyT](i:PiInstance[KeyT], process:String) extends PiEvent[KeyT] {
  override def id = i.id
	override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
			" === [" + id + "] FAILED - Unknown process: " + process
}
case class PiFailureAtomicProcessIsComposite[KeyT](i:PiInstance[KeyT], process:String) extends PiEvent[KeyT] {
  override def id = i.id
	override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
			" === [" + id + "] FAILED - Executor encountered composite process thread: " + process
}
case class PiFailureNoSuchInstance[KeyT](override val id:KeyT) extends PiEvent[KeyT] {
	override def asString = " === [" + id + "] FAILED - Failed to find instance!"
}
case class PiEventException[KeyT](override val id:KeyT, message:String, stackTrace:String) extends PiEvent[KeyT] {
	override def asString = " === [" + id + "] FAILED - Exception: " + message +
			"\n === [" + id + "] Trace: " + stackTrace
}
case class PiEventProcessException[KeyT](override val id:KeyT, ref:Int, message:String, stackTrace:String) extends PiEvent[KeyT] {
	override def asString = " === [" + id + "] PROCESS [" + ref + "] FAILED - Exception: " + message +
			"\n === [" + id + "] Trace: " + stackTrace
}

object PiEventException {
  def apply[KeyT](id:KeyT, ex:Throwable): PiEventException[KeyT] = PiEventException(id,ex.getLocalizedMessage,ExceptionUtils.getStackTrace(ex))
}
object PiEventProcessException {
  def apply[KeyT](id:KeyT, ref:Int, ex:Throwable): PiEventProcessException[KeyT] = PiEventProcessException(id,ref,ex.getLocalizedMessage,ExceptionUtils.getStackTrace(ex))
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
  override def apply(e:PiEvent[T]) = {
    System.err.println(e.asString)
    false
  }
}

class PromiseHandler[T](override val name:String, val id:T) extends PiEventHandler[T] {   
  val promise = Promise[Any]()
  def future = promise.future
  
  class PromiseException(message:String) extends Exception(message)
  
  override def apply(e:PiEvent[T]) = if (e.id == this.id) e match {  
    case PiEventResult(i,res) => promise.success(res); true
    case PiFailureNoResult(i) => promise.failure(new PromiseException(e.asString)); true 
    case PiFailureUnknownProcess(i, process) => promise.failure(new PromiseException(e.asString)); true
    case PiFailureAtomicProcessIsComposite(i, process) => promise.failure(new PromiseException(e.asString)); true  
    case PiFailureNoSuchInstance(id) => promise.failure(new PromiseException(e.asString)); true
    case PiEventException(id, message, stackTrace) => promise.failure(new PromiseException(e.asString)); true
    case PiEventProcessException(id, ref, message, stackTrace) => promise.failure(new PromiseException(e.asString)); true
    case _ => false
  } else false 
}

class PromiseHandlerFactory[T](name:T=>String) extends PiEventHandlerFactory[T,PromiseHandler[T]] {
  override def build(id:T) = new PromiseHandler[T](name(id),id)
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
  def subscribe(handler:PiEventHandler[T]):Unit
  def unsubscribe(handlerName:String):Unit
}

trait SimplePiObservable[T] extends PiObservable[T] {
  var handlers:Queue[PiEventHandler[T]] = Queue()
  
  override def subscribe(handler:PiEventHandler[T]):Unit = {
    System.err.println("Subscribed: " + handler.name)
    handlers = handlers :+ handler
  }
  
  override def unsubscribe(handlerName:String):Unit = {
    handlers = handlers filter (_.name!=handlerName)  
  }
  
  def publish(evt:PiEvent[T]) = {
    handlers = handlers filterNot (_(evt))
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe( handler: PiEventHandler[T] ): Unit = worker.subscribe( handler )
  override def unsubscribe( handlerName: String ): Unit = worker.unsubscribe( handlerName )
}