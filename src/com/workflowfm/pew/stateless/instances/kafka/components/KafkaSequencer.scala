package com.workflowfm.pew.stateless.instances.kafka.components

import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.stateless.components.StatelessComponent
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KafkaSequencerComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.StatelessKafkaSettings
import org.apache.kafka.common.TopicPartition

class KafkaSequencer(
    implicit sys: ActorSystem,
    mat: Materializer,
    settings: StatelessKafkaSettings

  ) extends KafkaSequencerComponent(
    settings.psAllMessages

  ) {

  import KafkaTopic._

  // Uses SequenceResponseBuilder instances instead of a stateless component.
  override def componentBuilder(router: StatelessRouter[MsgOut])
    : StatelessComponent[MsgOut] = null

  override def source: Source[MsgIn, Control] = null
  override def process: Source[WrapperOut, Control] = {

    // Synchronous handling for each PiiHistory partition this consumer handles.
    // Blocks until a ReduceRequest is released by receiving both a PiiUpdate
    // *and* at least one SequenceRequest.
    def handlePartition[Mat]( part: TopicPartition, src: Source[MsgIn, Mat] )
    : Source[WrapperOut, Mat] = {

      logger.info( "Booting up connector for: {}", part )

      src.map( msg => {
        logger.info( "{}: Received {}: {}", part, msg.record.value.getClass.getSimpleName, msg.record.key )
        msg
      })
      .scan( new SequenceResponseBuilder )(_ next _)
      .map( _.response )
      .collect({ case Some( multimsg ) =>

        // Log the ids of the PiInstances being reduced.
        val piiIds = multimsg.records.seq.map( _.key.asInstanceOf[KeyPiiId].toString )
        logger.info( part.toString +": Sending ReduceRequests: "+ piiIds.toString )

        multimsg
      })
    }

    // Grab the individual partition sources using a partitioned source.
    // Default group-id preserves consumer-offset so messages are not revisited.
    Consumer.committablePartitionedSource(
      settings.csPiiHistory,
      Subscriptions.topics( tnPiiHistory )

    // TODO: Figure out how to set the correct breadth.
    ).flatMapMerge( 4, t => handlePartition( t._1, t._2 ) )
  }
}
