package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiEventResult }

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.{ NotUsed, Done }

import scala.concurrent.{ Future, ExecutionContext }

/** A [[PiObservable]] implemented using an Akka Stream Source. */
trait PiSource[T] extends PiObservable[T] {
  implicit val materializer:Materializer

  def getSource:Source[PiEvent[T], NotUsed]

  def subscribeAndForget(f:PiEvent[T]=>Unit):Unit = getSource.runForeach(f)

  def subscribe(f:PiEvent[T]=>Unit):PiSwitch = {
    val kill = getSource
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.foreach(f))
      .run()
    PiKillSwitch(kill)
  }

}

/** A [[PiSource]] that also implements [[PiPublisher]] with Akka Streams.
  * Uses [[akka.stream.scaladsl.BroadcastHub]], which allows us to clone new sources and attach the
  * handlers as sinks.
  */
trait PiStream[T] extends PiSource[T] with PiPublisher[T] {
  implicit val system:ActorSystem
  override implicit val materializer = ActorMaterializer()

  private val sourceQueue = Source.queue[PiEvent[T]](PiStream.bufferSize, PiStream.overflowStrategy)

  private val (
    queue: SourceQueueWithComplete[PiEvent[T]],
    source: Source[PiEvent[T], NotUsed]
  ) = {
    val (q,s) = sourceQueue.toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both).run()
    s.runWith(Sink.ignore)
    (q,s)
  }

  override def publish(evt:PiEvent[T]) = queue.offer(evt)

  override def getSource = source
  //def subscribe(sink:PiEvent[T]=>Unit):Future[Done] = source.runForeach(sink(_))

  override def subscribe(handler:PiEventHandler[T]):Future[PiSwitch] = {
    // Using a shared kill switch even though we only want to shutdown one stream, because
    // the shared switch is available immediately (pre-materialization) so we can use it
    // as a sink.
    val killSwitch = KillSwitches.shared(s"kill:${handler.name}")
    val x = source
      .via(killSwitch.flow) // Use the killSwitch as a flow so we can kill the entire stream.
      .map(handler(_)) // Run the events through the handler.
      .runForeach { r => if (r) { // if the returned value is true, we need to "unsubscribe"
        killSwitch.shutdown() // we could not do this with a regular killSwitch, only with a shared one
      } }
    Future.successful(PiKillSwitch(killSwitch))
  }
}

object PiStream {
  val bufferSize = 5
  val overflowStrategy = OverflowStrategy.backpressure
}

/** A wrapper of [[akka.stream.KillSwitch]] to stop [[PiEventHandler]]s. */
case class PiKillSwitch(switch:KillSwitch) extends PiSwitch {
  override def stop = switch.shutdown()
}
