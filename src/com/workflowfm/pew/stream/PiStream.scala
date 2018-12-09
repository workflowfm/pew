package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiExceptionEvent, PiEventResult }

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.{ NotUsed, Done }

trait PiSource[T] extends PiPublisher[T] {
  implicit val system:ActorSystem
  implicit val materializer = ActorMaterializer()

  private val sourceQueue = Source.queue[PiEvent[T]](PiSource.bufferSize, PiSource.overflowStrategy)

  private val (
    queue: SourceQueueWithComplete[PiEvent[T]],
    source: Source[PiEvent[T], NotUsed]
  ) = {
    val (q,s) = sourceQueue.toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both).run()
    s.runWith(Sink.ignore)
    //s.runForeach(e => println(s">>>>>>>>>> ${e.id} ${e.time - 1542976910000L}"))
    (q,s)
  }

  protected def publish(evt:PiEvent[T]) = queue.offer(evt)

  def subscribe[R](sink:PiEvent[T]=>R):Unit = {
    val x = source
      .map(sink(_))
  }
  def subscribe(handler:PiEventHandler[T]):Unit = {
    // Using a shared kill switch even though we only want to shutdown one stream, because
    // the shared switch is available immediately (pre-materialization) so we can use it
    // as a sink.
    val killSwitch = KillSwitches.shared(s"kill:${handler.name}")
    val x = source
      .via(killSwitch.flow)
      .map { e => println(s">>> ${handler.name}: ${e.id} ${e.time- 1542976910000L}") ; e }
      .map(handler(_))
      .runForeach { r => if (r) {
        println(s"===KILLING IN THE NAME OF: ${handler.name}")
        killSwitch.shutdown() }
      }
  }
}

object PiSource {
  val bufferSize = 5
  val overflowStrategy = OverflowStrategy.backpressure
}


case class PiSink(name:String, switch:KillSwitch) {
  def shutdown = switch.shutdown()
}
