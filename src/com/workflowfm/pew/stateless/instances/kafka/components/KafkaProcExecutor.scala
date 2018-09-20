package com.workflowfm.pew.stateless.instances.kafka.components

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.stateless.components.{StatelessAtomicProcess, StatelessComponent}
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KafkaProcExecutorComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.StatelessKafkaSettings

class KafkaProcExecutor[ResultT](
    implicit val sys: ActorSystem,
    mat: Materializer,
    settings: StatelessKafkaSettings

  ) extends KafkaProcExecutorComponent(
    settings.psAllMessages

  ) {

  import KafkaTopic._

  override def componentBuilder(router: StatelessRouter[MsgOut])
    : StatelessComponent[MsgOut] = {

    implicit val implicitRouter: StatelessRouter[MsgOut] = router
    new StatelessAtomicProcess[MsgOut, ResultT]()
  }

  override def source: Source[MsgIn, Control] =
    toSource( tnAssignment, settings.csAssignment )
}
