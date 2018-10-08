package com.workflowfm.pew.stateless.instances.kafka.settings.bson

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.{ActorMaterializer, Materializer}
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.codecs.configuration.CodecRegistry

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

class BsonKafkaExecutorSettings(
    val reg: CodecRegistry,
    implicit override val actorSys: ActorSystem,
    override val execCtx: ExecutionContext = ExecutionContext.global

  ) extends KafkaExecutorSettings {

  import KafkaExecutorSettings._
  import com.workflowfm.pew.stateless.StatelessMessages._

  // Kafka - Topic Names
  val tnReduceRequest: TopicN = "ReduceRequest"
  val tnPiiHistory: TopicN = "PiiHistory"
  val tnAssignment: TopicN = "Assignment"
  val tnResult: TopicN = "Result"

  override implicit val mat: Materializer = ActorMaterializer.create( actorSys )

  val serverAndPort: String = "localhost:9092"
  val defaultGroupId: String = "Default-Group"

  def consSettings[K, V]( implicit ctK: ClassTag[K], ctV: ClassTag[V] )
    : ConsumerSettings[K, V] = {

    import ConsumerConfig._

    ConsumerSettings
    .create( actorSys, CodecWrapper[K](ctK, reg), CodecWrapper[V](ctV, reg) )
    .withBootstrapServers( serverAndPort )
    .withGroupId( defaultGroupId )
    .withProperty( AUTO_OFFSET_RESET_CONFIG, "earliest" )
    .withWakeupTimeout( 10.seconds )
  }

  def prodSettings[K, V]( implicit ctK: ClassTag[K], ctV: ClassTag[V] )
    : ProducerSettings[K, V] = {

    ProducerSettings
    .create( actorSys, CodecWrapper[K](ctK, reg), CodecWrapper[V](ctV, reg) )
    .withBootstrapServers( serverAndPort )
  }

  // Kafka - PiiId keyed consumer topic settings
  val csPiiHistory:       ConsumerSettings[KeyPiiId, PiiHistory]        = consSettings
  val csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest]   = consSettings
  val csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest]     = consSettings
  val csResult:           ConsumerSettings[KeyPiiId, PiiResult[AnyRes]] = consSettings

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment:       ConsumerSettings[KeyPiiIdCall, Assignment]    = consSettings

  // Kafka - All producer settings
  val psAllMessages:      ProducerSettings[AnyKey, AnyMsg]              = prodSettings

  override def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg] = {
    case m: PiiUpdate           => new ProducerRecord( tnPiiHistory, KeyPiiId(m.pii.id), m )
    case m: SequenceRequest     => new ProducerRecord( tnPiiHistory, KeyPiiId(m.piiId), m )
    case m: SequenceFailure     => new ProducerRecord( tnPiiHistory, KeyPiiId(m.piiId), m )
    case m: ReduceRequest       => new ProducerRecord( tnReduceRequest, KeyPiiId(m.pii.id), m )
    case m: Assignment          => new ProducerRecord( tnAssignment, KeyPiiIdCall(m.pii.id, m.callRef), m )
    case m: PiiResult[_]        => new ProducerRecord( tnResult, KeyPiiId(m.pii.id), m )
  }

}
