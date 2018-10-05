package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer.Control
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.components._
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings

import scala.concurrent.{ExecutionContext, Future}

/** Wrapper for a CustomKafkaExecutor that controls the lifecycle of an arbitrary collection
  * of local Kafka connectors.
  */
class CustomKafkaExecutor[ResultT]( components: Control* )( implicit settings: KafkaExecutorSettings )
  extends MinimalKafkaExecutor[ResultT]()( settings ) {

  val allControls: Seq[Control] = components

  override def shutdown: Future[Done] = KafkaConnectors.shutdownAll( allControls :+ eventHandlerControl )
}

/** Implements the full functionality of a Kafka Executor locally using the standard components.
  * - Independent Sequencer
  * - Independent Reducer
  * - Independent Atomic Process Executor
  */
object CompleteKafkaExecutor {

  import KafkaConnectors._

  def apply[ResultT]( implicit settings: KafkaExecutorSettings )
    : CustomKafkaExecutor[ResultT] = {

    implicit val execCtx: ExecutionContext = settings.execCtx

    new CustomKafkaExecutor[ResultT](
      indySequencer,
      indyReducer( new Reducer ),
      indyAtomicExecutor( AtomicExecutor() )
    )
  }

}

/** Implements the full functionality of a Kafka Executor locally using a combined Sequencer/Reducer.
  * - Joined Sequencer and Reducer connector.
  * - Independent Atomic Process Executor.
  */
object SeqRedKafkaExecutor {

  import KafkaConnectors._

  def apply[ResultT]( implicit settings: KafkaExecutorSettings )
    : CustomKafkaExecutor[ResultT] = {

    implicit val execCtx: ExecutionContext = settings.execCtx

    new CustomKafkaExecutor[ResultT](
      seqReducer( new Reducer ),
      indyAtomicExecutor( AtomicExecutor() )
    )
  }

}