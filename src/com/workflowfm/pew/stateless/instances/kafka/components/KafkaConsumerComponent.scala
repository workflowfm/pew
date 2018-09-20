package com.workflowfm.pew.stateless.instances.kafka.components

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage}
import akka.kafka.ProducerMessage.MultiMessage
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.stateless.components.StatelessComponent
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.TopicN
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

abstract class KafkaConsumerComponent[KIn, VIn, KOut, VOut](
    prodSettings: ProducerSettings[_, _]

  ) (
    implicit val system: ActorSystem,
    val materializer: Materializer,
    val execCtx: ExecutionContext = ExecutionContext.global

  ) extends StatelessRouter[ProducerRecord[KOut, VOut]] {

  type OurConsumerSettings = ConsumerSettings[KIn, VIn]
  type MsgIn = CommittableMessage[KIn, VIn]
  type MsgOut = ProducerRecord[KOut, VOut]
  type WrapperOut = MultiMessage[KOut, VOut, Committable]

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def componentBuilder( router: StatelessRouter[MsgOut] ): StatelessComponent[MsgOut]

  protected val component: StatelessComponent[MsgOut] = componentBuilder( this )

  // Jev, Made purely abstract
  // Create all input topic sources, then merge all together to create a single source.
  protected def source: Source[MsgIn, Control]

  // Execute graph.
  protected def process: Source[WrapperOut, Control]
    = source.mapAsync[WrapperOut](5 )( componentReceive )

  protected val sink: Sink[WrapperOut, Future[Done]]
    = Producer.commitableSink( prodSettings.asInstanceOf[ProducerSettings[KOut, VOut]] )


  // TODO: Transactions for exactly-once-delivery
  // https://doc.akka.io/docs/akka-stream-kafka/current/transactions.html

  /** Create and start a Kafka-Consumer to Kafka-Producer Akka stream with the specified consumer
    * and producer settings, using our stateless component to handle responses.
    */
  protected val control: Control =
    process
    .toMat( sink )( Keep.both )         // Write message responses.
    .mapMaterializedValue( DrainingControl.apply )  // Add shutdown control object.
    .named( getClass.toString )                     // Name for debugging.
    .run()

  protected def toSource( topic: TopicN, settings: ConsumerSettings[_, _], par: Option[Int] = None )
    : Source[MsgIn, Control] = {

    val groupId = settings.getProperty( ConsumerConfig.GROUP_ID_CONFIG )
    val maxPar = settings.getProperty( ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG )
    logger.trace( "New Consumer - Topic: '"+ topic +"', Group: '"+ groupId +"', Par: "+ par +"/"+ maxPar +"." )

    val castSettings = settings.asInstanceOf[OurConsumerSettings]
    val subscription = par match {
      case Some( partition )    => Subscriptions.assignment( new TopicPartition( topic, partition ) )
      case None                 => Subscriptions.topics( topic )
    }

    Consumer.committableSource( castSettings, subscription )
  }


  protected def componentReceive[Mat]( msg: MsgIn )
    : Future[WrapperOut] = {

    try {
      logger.trace( "Receiving message: " + msg.record.value.toString )
      val response = component.receive( msg.record.value )
      logger.trace( "Handled message: " + msg.record.value.toString )

      response.map(
        ( msgs: Seq[MsgOut] ) =>
          MultiMessage( msgs.to, msg.committableOffset )
      )

    } catch {
      case e: Exception =>
        e.printStackTrace()
        Future.failed(e)
    }
  }

  override def route(msg: Any): MsgOut = {
    try {
      val typelessMsg = KafkaTopic.toProducerMessage( msg ).asInstanceOf[MsgOut]
      logger.info( "Sending message: " + typelessMsg.toString )
      typelessMsg

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

  def shutdown: Future[Done] = control.shutdown

  final def syncShutdown( timeout: Duration = Duration.Inf ): Done
    = Await.result( shutdown, timeout )
}
