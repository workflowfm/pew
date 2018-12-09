package com.workflowfm.pew.stream

import com.workflowfm.pew.PiEvent

import scala.concurrent.{ Promise, Future, ExecutionContext }


trait PiObservable[T] {
  def subscribe(handler:PiEventHandler[T]):Future[Boolean]
  def unsubscribe(handlerName:String):Future[Boolean]
}

trait SimplePiObservable[T] extends PiObservable[T] {
  import collection.mutable.Map

  implicit val executionContext:ExecutionContext

  val handlers:Map[String,PiEventHandler[T]] = Map[String,PiEventHandler[T]]()
  
  override def subscribe(handler:PiEventHandler[T]):Future[Boolean] = Future {
    //System.err.println("Subscribed: " + handler.name)
    handlers += (handler.name -> handler)
    true
  }
  
  override def unsubscribe(handlerName:String):Future[Boolean] = Future {
    handlers.remove(handlerName).isDefined
  }
  
  def publish(evt:PiEvent[T]) = {
    handlers.retain((k,v) => !v(evt))
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe( handler: PiEventHandler[T] ): Future[Boolean] = worker.subscribe( handler )
  override def unsubscribe( handlerName: String ): Future[Boolean] = worker.unsubscribe( handlerName )
}
