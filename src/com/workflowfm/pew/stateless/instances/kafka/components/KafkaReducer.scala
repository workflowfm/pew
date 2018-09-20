package com.workflowfm.pew.stateless.instances.kafka.components

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.stateless.components.{StatelessComponent, StatelessReducer}
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KafkaReducerComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.StatelessKafkaSettings

class KafkaReducer(
    implicit val sys: ActorSystem,
    mat: Materializer,
    settings: StatelessKafkaSettings

  ) extends KafkaReducerComponent(
    settings.psAllMessages

  ) {

  import KafkaTopic._

  override def componentBuilder(router: StatelessRouter[MsgOut])
    : StatelessComponent[MsgOut] = {

    implicit val implicitRouter: StatelessRouter[MsgOut] = router
    new StatelessReducer[MsgOut]()
  }

  override def source: Source[MsgIn, Control] =
    toSource( tnReduceRequest, settings.csReduceRequest )
}
