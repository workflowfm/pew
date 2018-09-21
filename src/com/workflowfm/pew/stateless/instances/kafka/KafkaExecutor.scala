package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer.Control
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings

import scala.concurrent.Future

/** Complete KafkaExecutor that implements all KafkaComponents internally.
  *
  * @param processes
  * @param settings
  * @tparam ResultT
  */
class KafkaExecutor[ResultT](processes: PiProcessStore, components: (() => Control)* )(implicit settings: KafkaExecutorSettings)
  extends MinimalKafkaExecutor[ResultT]( processes )( settings ) {

  val allControls: Seq[Control] = components map (_())

  override def shutdown: Future[Done] = KafkaConnectors.shutdown( allControls :+ eventHandlerControl )
}


