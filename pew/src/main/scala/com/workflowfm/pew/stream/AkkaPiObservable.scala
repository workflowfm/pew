package com.workflowfm.pew.stream

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.util.Timeout

import com.workflowfm.pew.{ PiEvent }

trait AkkaPiStream[T] extends PiPublisher[T] { actor: Actor =>
  import collection.mutable.HashSet

  implicit val executionContext: ExecutionContext

  protected val handlers: HashSet[ActorRef] = HashSet[ActorRef]()

  def subscribe(handler: ActorRef): Unit = {
    handlers += handler
  }

  def unsubscribe(handler: ActorRef): Unit = {
    handlers -= handler
  }

  override def publish(evt: PiEvent[T]): Unit = {
    handlers.foreach(_ ! evt)
  }

  def publisherReceive: Receive = {
    case AkkaPiStream.Subscribe(sub) => sender() ! subscribe(sub)
    case AkkaPiStream.Unsubscribe(sub) => unsubscribe(sub)
  }

}

object AkkaPiStream {
  case class Subscribe(sub: ActorRef)
  case class Unsubscribe(sub: ActorRef)
}

case class AkkaPiSwitch(pub: ActorRef, sub: ActorRef) extends PiSwitch {
  override def stop(): Unit = pub ! AkkaPiStream.Unsubscribe(sub)
}

class PiSubscriber[T](handler: PiEventHandler[T], publisher: ActorRef) extends Actor {
  val switch: PiSwitch = AkkaPiSwitch(publisher, self)

  def onEvent(event: PiEvent[T]): Unit =
    if (handler(event)) switch.stop()

  override def receive: Receive = {
    case evt: PiEvent[T] => onEvent(evt)
  }
}

object PiSubscriber {

  def props[T](handler: PiEventHandler[T], publisher: ActorRef): Props = {
    Props(new PiSubscriber(handler, publisher))
  }
}

trait AkkaPiObservable[T] extends PiObservable[T] {
//  implicit val tag: ClassTag[PiEvent[T]]
  implicit val system: ActorSystem
  implicit val timeout: FiniteDuration

  val publisher: ActorRef

  override def subscribe(handler: PiEventHandler[T]): Future[PiSwitch] = {
    val sub: ActorRef = system.actorOf(PiSubscriber.props(handler, publisher))
    val switch = AkkaPiSwitch(publisher, sub)
    (publisher ? AkkaPiStream.Subscribe(sub))(Timeout(timeout))
      .map({ _ => switch })(system.dispatcher)
  }
}
