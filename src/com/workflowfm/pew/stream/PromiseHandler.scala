package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiException, PiExceptionEvent, PiEventResult }

import scala.concurrent.{ Promise, Future, ExecutionContext }


trait PromiseHandler[T,R] extends PiEventHandler[T] {   
  val id:T

  protected val promise = Promise[R]()
  def future = promise.future
  
  // class PromiseException(message:String) extends Exception(message)
  
  override def apply(e:PiEvent[T]) = if (e.id == this.id) update(e) match {
    case PiEventResult(i,res,_) => promise.success(succeed(res)); true
    case ex: PiExceptionEvent[T] => fail(ex.exception) match {
      case Left(r) => promise.success(r); true
      case Right(x) => promise.failure(x); true
    }
    case _ => false
  } else false

  /**
    * This should handle the event (if needed) and return it, potentially updated.
    * (Default does nothing.)
    */
  def update(event:PiEvent[T]) :PiEvent[T] = event

  /**
    * This will be executed when the workflow completes successfully.
    * @param result the result of the workflow
    * @return an object of type R that will complete the promise
    */
  def succeed(result:Any) :R

  /**
    * This will be executed when the workflow fails due to an exception.
    * @param exception the exception that occurred
    * @return either an object to complete the promise successfully or an exception to fail the promise with
    */
  def fail(exception:PiException[T]) :Either[R,Exception]
}

class ResultHandler[T](override val name:String, override val id:T) extends PromiseHandler[T,Any] {

  override def succeed(result:Any) = result
  override def fail(exception:PiException[T]) = Right(exception)
}

class ResultHandlerFactory[T](name:T=>String) extends PiEventHandlerFactory[T,ResultHandler[T]] {
  def this(name:String) = this { _:T => name }
  override def build(id:T) = new ResultHandler[T](name(id),id)
}



class CounterHandler[T](override val name:String, override val id:T) extends PromiseHandler[T,Int] {   
  private var counter:Int = 0
  def count = counter

  override def update(event:PiEvent[T]) = {
    counter += 1
    // TODO add metadata for the counter here, because why not?
    event
  }

  override def succeed(result:Any) = counter
  override def fail(exception:PiException[T]) = Left(counter)
}

class CounterHandlerFactory[T](name:T=>String) extends PiEventHandlerFactory[T,CounterHandler[T]] {
  def this(name:String) = this { _:T => name }
  override def build(id:T) = new CounterHandler[T](name(id),id)
}


