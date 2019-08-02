package com.workflowfm.pew.stream

import akka.actor.ActorRef
import akka.stream.KillSwitch
import com.workflowfm.pew.{ PiEvent, PiEventResult }
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
import uk.ac.ed.inf.ppapapan.subakka.{ Publisher, Subscriber }

class PiSubscriber[T](handler: PiEventHandler[T]) extends Subscriber[PiEvent[T]] {
  var switch: Option[KillSwitch] = None

  override def onInit(publisher: ActorRef, k: KillSwitch): Unit = switch = Some(k)
  override def onEvent(event: PiEvent[T]): Unit = if (handler(event)) switch.map(_.shutdown())
}

/** A [[PiSource]] that also implements [[PiPublisher]] with Akka Streams.
  * Uses [[akka.stream.scaladsl.BroadcastHub]], which allows us to clone new sources and attach the
  * handlers as sinks.
  */
trait PiStream[T] extends PiPublisher[T] with Publisher[PiEvent[T]] with PiObservable[T] {
  implicit val tag: ClassTag[PiEvent[T]]
  implicit val executionContext: ExecutionContext

  override def subscribe(handler:PiEventHandler[T]):Future[PiSwitch] = {
    new PiSubscriber[T](handler).subscribeTo(self)(context.system,tag) map { i => PiKillSwitch(i.killSwitch) }
  }
}


/** A wrapper of [[akka.stream.KillSwitch]] to stop [[PiEventHandler]]s. */
case class PiKillSwitch(switch: KillSwitch) extends PiSwitch {
  override def stop = switch.shutdown()
}
