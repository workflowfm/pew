package com.workflowfm.pew

import scala.concurrent.{Promise,Future}
import scala.collection.immutable.Queue

sealed trait PiEvent[KeyT] {
  def id:KeyT
}

case class PiEventStart[KeyT](i:PiInstance[KeyT]) extends PiEvent[KeyT] {
  override def id = i.id
}
case class PiEventResult[KeyT](i:PiInstance[KeyT], res:Any) extends PiEvent[KeyT] {
  override def id = i.id
}
case class PiEventFailure[KeyT](i:PiInstance[KeyT], reason:Throwable) extends PiEvent[KeyT] {
  override def id = i.id
}
case class PiEventException[KeyT](override val id:KeyT, reason:Throwable) extends PiEvent[KeyT] 
case class PiEventCall[KeyT](override val id:KeyT, ref:Int, p:AtomicProcess, args:Seq[PiObject]) extends PiEvent[KeyT]
case class PiEventReturn[KeyT](override val id:KeyT, ref:Int, result:Any) extends PiEvent[KeyT]
case class PiEventProcessException[KeyT](override val id:KeyT, ref:Int, reason:Throwable) extends PiEvent[KeyT]

// Return true if the handler is done and needs to be unsubscribed.

trait PiEventHandler[KeyT] extends (PiEvent[KeyT]=>Boolean) {
  def name:String
  def and(h:PiEventHandler[KeyT]) = MultiPiEventHandler(this,h)
}

trait PiEventHandlerFactory[T] {
  def build(id:T):PiEventHandler[T]
}

class PrintEventHandler[T] extends PiEventHandler[T] {   
  override def apply(e:PiEvent[T]) = { e match {
    case PiEventStart(i) => System.err.println(" === INITIAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    case PiEventResult(i,res) => {
      System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
      System.err.println(" === RESULT FOR " + i.id + ": " + res)
    }
    case PiEventFailure(i,reason) => {	  
    	  System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    	  System.err.println(" === FAILED: " + i.id + " ! === Exception: " + reason)
    	  reason.printStackTrace()
    }
    case PiEventException(id,reason) => {	  
  	    System.err.println(" === EXCEPTION: " + id + " ! === Exception: " + reason)
  	    reason.printStackTrace()
    }
    case PiEventCall(id,ref,p,args) => System.err.println(" === PROCESS CALL " + id + " === \n" + p.name + " (" + ref + ") with args: " + args.mkString(",") + "\n === === === === === === === ===")
    case PiEventReturn(id,ref,result) => System.err.println(" === PROCESS RETURN " + id + " === \n" + ref + " returned: " + result + "\n === === === === === === === ===")
    case PiEventProcessException(id,ref,reason) => {	  
    	  System.err.println(" === PROCESS FAILED: " + id + " === " + ref + " === Exception: " + reason)
    	  reason.printStackTrace()
     }   
  }
  false }
}

class PromiseHandler[T](override val name:String, val id:T) extends PiEventHandler[T] {   
  val promise = Promise[Any]()
  def future = promise.future
  
  override def apply(e:PiEvent[T]) = if (e.id == this.id) e match { 
    case PiEventResult(i,res) => promise.success(res); true
    case PiEventFailure(i,reason) => promise.failure(reason); true
    case PiEventException(id,reason) => promise.failure(reason); true
    case PiEventProcessException(id,ref,reason) => promise.failure(reason); true
    case _ => false
  } else false 
}

class PromiseHandlerFactory[T](name:String) extends PiEventHandlerFactory[T] {
  override def build(id:T) = new PromiseHandler[T](name,id)
}

case class MultiPiEventHandler[T](handlers:Queue[PiEventHandler[T]]) extends PiEventHandler[T] {
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
  
  override def subscribe(handler:PiEventHandler[T]):Unit = 
    handlers = handlers :+ handler
  
  override def unsubscribe(handlerName:String):Unit = {
    handlers = handlers filter (_.name!=handlerName)  
  }
  
  def publish(evt:PiEvent[T]) = {
    handlers = handlers filterNot (_(evt))
  }
}