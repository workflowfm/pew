package com.workflowfm.pew.stateless

import akka.{Done, NotUsed}
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Flow, Sink}
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, ReduceRequest}
import com.workflowfm.pew.stateless.components.{AtomicExecutor, Reducer, ResultListener}
import com.workflowfm.pew.stateless.instances.kafka.CustomKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object TestKafkaConnectors {

  def flowFlatten[T]: Flow[Seq[T], T, NotUsed]
    = Flow[Seq[T]].mapConcat[T]( _.to )

  def checkMsg[T <: AnyMsg](): Flow[T, AnyMsg, NotUsed] = {
    val outputSet: mutable.Set[T] = mutable.Set[T]()
    Flow[T]
    .map(m =>
      if (outputSet contains m)
        Some( m )
      else {
        outputSet add m
        None
      }

    ).collect({ case Some(m) => m })
  }

  def indyReducer( red: Reducer, sink: Sink[AnyMsg, Future[Done]] )( implicit s: KafkaExecutorSettings ): Control
    = {
    val flowCheck
      = flowUntrack[Seq[AnyMsg]]
        .via( flowFlatten )
        //.via( checkMsg() )
        .to( sink )

    run(
      srcReduceRequest
      via flowRespond(red)
      wireTap flowCheck
      via flowMultiMessage,
      sinkProducerMsg
    )
  }

  def indySequencer( sink: Sink[AnyMsg, Future[Done]] )( implicit s: KafkaExecutorSettings ): Control
    = {
    val flowCheck
      = flowUntrack[Seq[ReduceRequest]]
        .via( flowFlatten )
        // .via( checkMsg() )
        .to( sink )

    run(
      srcPiiHistory( flowSequencer )
      wireTap flowCheck
      via flowMultiMessage,
      sinkProducerMsg
    )
  }

  def indyAtomicExecutor( exec: AtomicExecutor, sink: Sink[AnyMsg, Future[Done]] )( implicit s: KafkaExecutorSettings ): Control
    = {
    val flowCheck
      = flowUntrack[AnyMsg]
        // .via( checkMsg() )
        .to( sink )

    run(
      srcAssignment
      via flowRespond( exec )
      via flowWaitFuture( 1 )
      wireTap flowCheck
      via flowMessage,
      sinkProducerMsg
    )
  }
}

object TestKafkaExecutor {

  import TestKafkaConnectors._

  def apply[ResultT]( sink: Sink[AnyMsg, Future[Done]] )( implicit settings: KafkaExecutorSettings )
    : CustomKafkaExecutor[ResultT] = {

    implicit val execCtx: ExecutionContext = settings.execCtx

    new CustomKafkaExecutor[ResultT](
      indySequencer( sink ),
      indyReducer( new Reducer, sink ),
      indyAtomicExecutor( AtomicExecutor(), sink )
    )
  }

}