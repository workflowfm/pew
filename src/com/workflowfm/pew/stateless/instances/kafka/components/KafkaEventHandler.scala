package com.workflowfm.pew.stateless.instances.kafka.components

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.workflowfm.pew.PiEventHandler
import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.stateless.components.{StatelessComponent, StatelessEventHandler}
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KafkaEventHandlerComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.StatelessKafkaSettings
import org.bson.types.ObjectId
import org.mongodb.scala.bson.ObjectId

class KafkaEventHandler[ResultT](
    handlers: Seq[PiEventHandler[ObjectId, _]]

  )(
    implicit val sys: ActorSystem,
    implicit val mat: Materializer,
    settings: StatelessKafkaSettings

  ) extends KafkaEventHandlerComponent(
    settings.psAllMessages
  ) {

  import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic._

  override def componentBuilder(router: StatelessRouter[MsgOut])
    : StatelessComponent[MsgOut] = {

    implicit val implicitRouter: StatelessRouter[MsgOut] = router
    new StatelessEventHandler[ResultT, MsgOut]( handlers )
  }

  /** Override `source` to give handler a uniqueGroupId so it each KafkaEventHandler
    * component can listen to all events on the cluster. StatelessEventHandler is
    * responsible for ignoring the irrelevant messages appropriately.
    */
  override def source: Source[MsgIn, Control] = {
    val uniqueGroupId: String = "Event-Group-" + ObjectId.get.toHexString
    toSource( tnResult, settings.csResult withGroupId uniqueGroupId )
  }

}
