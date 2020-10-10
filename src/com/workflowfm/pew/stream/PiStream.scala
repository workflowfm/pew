package com.workflowfm.pew.stream

import akka.actor.ActorRef
import akka.stream.KillSwitch
import com.workflowfm.pew.{ PiEvent, PiEventResult }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import uk.ac.ed.inf.ppapapan.subakka.{ HashSetPublisher, Subscriber, SubscriptionSwitch }

class PiSubscriber[T](handler: PiEventHandler[T]) extends Subscriber[PiEvent[T]] {
  var switch: Option[SubscriptionSwitch] = None

  override def onInit(publisher: ActorRef, killSwitch: SubscriptionSwitch): Unit = switch = Some(
    killSwitch
  )
  override def onEvent(event: PiEvent[T]): Unit = if (handler(event)) switch.map(_.stop())
}

/** A [[PiSource]] that also implements [[PiPublisher]] with Akka Streams.
  * Uses [[akka.stream.scaladsl.BroadcastHub]], which allows us to clone new sources and attach the
  * handlers as sinks.
  */
trait PiStream[T] extends PiPublisher[T] with HashSetPublisher[PiEvent[T]] with PiObservable[T] {
  implicit val tag: ClassTag[PiEvent[T]]
  implicit val executionContext: ExecutionContext
  implicit val timeout: FiniteDuration

  override def subscribe(handler: PiEventHandler[T]): Future[PiSwitch] = {
    new PiSubscriber[T](handler).subscribeTo(self, None, timeout)(context.system, tag) map { i =>
      PiKillSwitch(i.killSwitch)
    }
  }
}

/** A wrapper of [[akka.stream.KillSwitch]] to stop [[PiEventHandler]]s. */
case class PiKillSwitch(switch: SubscriptionSwitch) extends PiSwitch {
  override def stop = switch.stop()
}
