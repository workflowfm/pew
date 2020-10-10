package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiEventResult, PiException, PiFailure }

import scala.concurrent.{ ExecutionContext, Future, Promise }

trait PromiseHandler[T, R] extends PiEventHandler[T] {
  val id: T

  protected val promise = Promise[R]()
  def future = promise.future

  // class PromiseException(message:String) extends Exception(message)

  override def apply(e: PiEvent[T]) = if (e.id == this.id) update(e) match {
    case PiEventResult(i, res, _) => promise.success(succeed(res)); true
    case ex: PiFailure[T] =>
      fail(ex.exception) match {
        case Left(r) => promise.success(r); true
        case Right(x) => promise.failure(x); true
      }
    case _ => false
  }
  else false

  /**
    * This should handle the event (if needed) and return it, potentially updated.
    * (Default does nothing.)
    */
  def update(event: PiEvent[T]): PiEvent[T] = event

  /**
    * This will be executed when the workflow completes successfully.
    * @param result the result of the workflow
    * @return an object of type R that will complete the promise
    */
  def succeed(result: Any): R

  /**
    * This will be executed when the workflow fails due to an exception.
    * @param exception the exception that occurred
    * @return either an object to complete the promise successfully or an exception to fail the promise with
    */
  def fail(exception: PiException[T]): Either[R, Exception]
}

class ResultHandler[T](override val id: T) extends PromiseHandler[T, Any] {

  override def succeed(result: Any) = result
  override def fail(exception: PiException[T]) = Right(exception)
}

class ResultHandlerFactory[T] extends PiEventHandlerFactory[T, ResultHandler[T]] {
  override def build(id: T) = new ResultHandler[T](id)
}

class CounterHandler[T](override val id: T) extends PromiseHandler[T, Int] {
  private var counter: Int = 0
  def count = counter

  override def update(event: PiEvent[T]) = {
    counter += 1
    // TODO add metadata for the counter here, because why not?
    event
  }

  override def succeed(result: Any) = counter
  override def fail(exception: PiException[T]) = Left(counter)
}

class CounterHandlerFactory[T] extends PiEventHandlerFactory[T, CounterHandler[T]] {
  override def build(id: T) = new CounterHandler[T](id)
}
