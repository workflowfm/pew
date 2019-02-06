package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import com.workflowfm.pew.stateless.components._
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors.DrainControl
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings

import scala.concurrent.{ExecutionContext, Future}

/** Wrapper for a CustomKafkaExecutor that controls the lifecycle of an arbitrary collection
  * of local Kafka connectors.
  */
class CustomKafkaExecutor( components: DrainControl* )( implicit settings: KafkaExecutorSettings )
  extends MinimalKafkaExecutor()( settings ) {

  val allControls: Seq[DrainControl] = eventHandlerControl +: components.toSeq
  override def shutdown: Future[Done] = KafkaConnectors.drainAndShutdownAll( allControls )
  override def forceShutdown: Future[Done] = KafkaConnectors.shutdownAll( allControls )
}

/** Implements the full functionality of a Kafka Executor locally using the standard components.
  * - Independent Sequencer
  * - Independent Reducer
  * - Independent Atomic Process Executor
  */
object CompleteKafkaExecutor {

  import KafkaConnectors._

  def apply[ResultT]( implicit settings: KafkaExecutorSettings )
    : CustomKafkaExecutor = {

    implicit val executionContext: ExecutionContext = settings.executionContext

    new CustomKafkaExecutor(
      indySequencer,
      indyReducer( new Reducer ),
      indyAtomicExecutor( new AtomicExecutor() )
    )
  }

}

/** Implements the full functionality of a Kafka Executor locally using a combined Sequencer/Reducer.
  * - Joined Sequencer and Reducer connector.
  * - Independent Atomic Process Executor.
  */ /* TODO: Reimplement when seqReducers are a thing again.
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
} */
