package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiExceptionEvent, PiEventResult }

import scala.concurrent.{ Promise, Future, ExecutionContext }


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


