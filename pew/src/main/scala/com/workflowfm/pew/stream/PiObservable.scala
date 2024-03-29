package com.workflowfm.pew.stream

import scala.concurrent.{ ExecutionContext, Future, Promise }

import com.workflowfm.pew.PiEvent

/** Has the ability to publish [[PiEvent]]s.
  * This is separate from [[PiObservable]] as in some cases publishing events
  * and handling listeners happens in 2 different places.
  */
trait PiPublisher[T] {
  protected def publish(evt: PiEvent[T]): Unit
}

/** A kill switch allowing us to stop a [[PiEventHandler]]. */
trait PiSwitch {
  def stop(): Unit
}

/** Anything that can be observed by a [[PiEventHandler]].
  * This is separate from [[PiPublisher]] as in some cases publishing events
  * and handling listeners happens in 2 different places.
  */
trait PiObservable[T] {
  /** Subscribes a [[com.workflowfm.pew.stream.PiEventHandler]] to observe.
    * @param handler the handler to subscribe
    * @return the [[com.workflowfm.pew.stream.PiSwitch]] that allows us to stop/unsubscribe the subscribed handler
    */
  def subscribe(handler: PiEventHandler[T]): Future[PiSwitch]
}

/** A simple [[PiObservable]] and [[PiPublisher]] with a mutable map of handlers. */
trait SimplePiObservable[T] extends PiObservable[T] with PiPublisher[T] {
  import collection.mutable.Map

  implicit val executionContext: ExecutionContext

  protected val handlers: Map[String, PiEventHandler[T]] = Map[String, PiEventHandler[T]]()

  override def subscribe(handler: PiEventHandler[T]): Future[PiSwitch] = Future {
    val name = java.util.UUID.randomUUID.toString
    handlers += (name -> handler)
    Switch(name)
  }

  def unsubscribe(handlerName: String): Future[Boolean] = Future {
    handlers.remove(handlerName).isDefined
  }

  override def publish(evt: PiEvent[T]): Unit = {
    handlers.retain((k, v) => !v(evt))
  }

  case class Switch(name: String) extends PiSwitch {
    override def stop: Unit = unsubscribe(name)
  }
}

trait DelegatedPiObservable[T] extends PiObservable[T] {
  val worker: PiObservable[T]

  override def subscribe(handler: PiEventHandler[T]): Future[PiSwitch] = worker.subscribe(handler)
}
