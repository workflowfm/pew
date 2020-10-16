package com.workflowfm.pew.kafka

import scala.concurrent.ExecutionContext

import com.workflowfm.pew.kafka.components.KafkaConnectors
import com.workflowfm.pew.kafka.components.KafkaConnectors.DrainControl
import com.workflowfm.pew.kafka.settings.{ KafkaExecutorEnvironment, KafkaExecutorSettings }
import com.workflowfm.pew.stateless.components._

/** Wrapper for a CustomKafkaExecutor that controls the lifecycle of an arbitrary collection
  * of local Kafka connectors.
  */
class CustomKafkaExecutor(components: DrainControl*)(implicit env: KafkaExecutorEnvironment)
    extends MinimalKafkaExecutor()(env) {

  override lazy val allControls: Seq[DrainControl] = eventHandlerControl +: components.toSeq
}

/** Implements the full functionality of a Kafka Executor locally using the standard components.
  * - Independent Sequencer
  * - Independent Reducer
  * - Independent Atomic Process Executor
  */
object CompleteKafkaExecutor {

  import KafkaConnectors._

  def apply[ResultT](implicit settings: KafkaExecutorSettings): CustomKafkaExecutor = {

    implicit val env: KafkaExecutorEnvironment = settings.createEnvironment()
    implicit val executionContext: ExecutionContext = env.context

    new CustomKafkaExecutor(
      indySequencer,
      indyReducer(new Reducer),
      indyAtomicExecutor(new AtomicExecutor())
    )
  }

}

/** Implements the full functionality of a Kafka Executor locally using a combined Sequencer/Reducer.
  * - Joined Sequencer and Reducer connector.
  * - Independent Atomic Process Executor.
  */ /* TODO: Reimplement when seqReducers are a thing again.
 * object SeqRedKafkaExecutor {
 *
 * import KafkaConnectors._
 *
 * def apply[ResultT]( implicit settings: KafkaExecutorSettings ) : CustomKafkaExecutor[ResultT] = {
 *
 * implicit val execCtx: ExecutionContext = settings.execCtx
 *
 * new CustomKafkaExecutor[ResultT]( seqReducer( new Reducer ), indyAtomicExecutor( AtomicExecutor()
 * ) ) } } */
