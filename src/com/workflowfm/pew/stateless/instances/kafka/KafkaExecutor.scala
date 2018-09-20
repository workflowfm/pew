package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.instances.kafka.components.{KafkaProcExecutor, KafkaReducer, KafkaSequencer}
import com.workflowfm.pew.stateless.instances.kafka.settings.StatelessKafkaSettings

import scala.concurrent.Future

/** Complete KafkaExecutor that implements all KafkaComponents internally.
  *
  * @param processes
  * @param settings
  * @tparam ResultT
  */
class KafkaExecutor[ResultT](processes: PiProcessStore)(implicit settings: StatelessKafkaSettings)
  extends MinimalKafkaExecutor[ResultT]( processes )( settings ) {

  import KafkaTopic._

  // All necessary KafkaComponent instances.
  val reducer: KafkaReducerComponent = new KafkaReducer
  val processExecutor: KafkaProcExecutorComponent = new KafkaProcExecutor
  val sequencer: KafkaSequencerComponent = new KafkaSequencer

  override def shutdown: Future[Done] = {
    val reducerShutdown = reducer.shutdown
    val processShutdown = processExecutor.shutdown
    val sequencerShutdown = sequencer.shutdown

    super.shutdown
    .flatMap( _ => reducerShutdown )
    .flatMap( _ => processShutdown )
    .flatMap( _ => sequencerShutdown )
  }
}