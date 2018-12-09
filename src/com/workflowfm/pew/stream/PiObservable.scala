package com.workflowfm.pew.stream

import com.workflowfm.pew.PiEvent

import scala.concurrent.{ Promise, Future, ExecutionContext }

trait PiPublisher[T] {
  protected def publish(evt:PiEvent[T]):Unit
}

trait PiSwitch {
  def stop:Unit
}

trait PiObservable[T] {
  def subscribe(handler:PiEventHandler[T]):Future[PiSwitch]
}


trait SimplePiObservable[T] extends PiObservable[T] with PiPublisher[T] {
  import collection.mutable.Map

  implicit val executionContext:ExecutionContext

  protected val handlers:Map[String,PiEventHandler[T]] = Map[String,PiEventHandler[T]]()
  
  override def subscribe(handler:PiEventHandler[T]):Future[PiSwitch] = Future {
    //System.err.println("Subscribed: " + handler.name)
    handlers += (handler.name -> handler)
    Switch(handler.name)
  }
  
  def unsubscribe(handlerName:String):Future[Boolean] = Future {
    handlers.remove(handlerName).isDefined
  }
  
  override def publish(evt:PiEvent[T]) = {
    handlers.retain((k,v) => !v(evt))
  }

  case class Switch(name:String) extends PiSwitch {
    override def stop:Unit = unsubscribe(name)
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe( handler: PiEventHandler[T] ): Future[PiSwitch] = worker.subscribe( handler )
}
