package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiExceptionEvent, PiEventResult }

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.{ NotUsed, Done }

import scala.concurrent.{ Future, ExecutionContext }

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

trait PiStream[T] extends PiSource[T] with PiPublisher[T] {
  implicit val system:ActorSystem
  override implicit val materializer = ActorMaterializer()

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

  override def publish(evt:PiEvent[T]) = queue.offer(evt)

  override def getSource = source
  //def subscribe(sink:PiEvent[T]=>Unit):Future[Done] = source.runForeach(sink(_))
  
  override def subscribe(handler:PiEventHandler[T]):Future[PiSwitch] = {
    // Using a shared kill switch even though we only want to shutdown one stream, because
    // the shared switch is available immediately (pre-materialization) so we can use it
    // as a sink.
    val killSwitch = KillSwitches.shared(s"kill:${handler.name}")
    val x = source
      .via(killSwitch.flow)
      //.map { e => println(s">>> ${handler.name}: ${e.id} ${e.time- 1542976910000L}") ; e }
      .map(handler(_))
      .runForeach { r => if (r) {
        //println(s"===KILLING IN THE NAME OF: ${handler.name}")
        killSwitch.shutdown() }
      }
    Future.successful(PiKillSwitch(killSwitch))
  }
}

object PiSource {
  val bufferSize = 5
  val overflowStrategy = OverflowStrategy.backpressure
}


case class PiKillSwitch(switch:KillSwitch) extends PiSwitch {
  override def stop = switch.shutdown()
}
