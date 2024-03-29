package com.workflowfm.pew.kafka.settings.bson

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor.ActorSystem
import akka.kafka.{ ConsumerSettings, ProducerSettings }
import akka.stream.{ ActorMaterializer, Materializer }
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.codecs.configuration.CodecRegistry

import com.workflowfm.pew.kafka.settings.{ KafkaExecutorEnvironment, KafkaExecutorSettings }

class BsonKafkaExecutorSettings(val reg: CodecRegistry) extends KafkaExecutorSettings {

  import KafkaExecutorSettings._

  import com.workflowfm.pew.stateless.StatelessMessages._

  // Kafka - Topic Names
  override val tnReduceRequest: TopicN = "ReduceRequest"
  override val tnPiiHistory: TopicN = "PiiHistory"
  override val tnAssignment: TopicN = "Assignment"
  override val tnResult: TopicN = "Result"

  override val serverAndPort: String = "localhost:9092"
  override val defaultGroupId: String = "Default-Group"

  def consSettings[K, V](
      implicit ctK: ClassTag[K],
      ctV: ClassTag[V],
      actorSys: ActorSystem
  ): ConsumerSettings[K, V] = {

    import ConsumerConfig._

    ConsumerSettings
      .create(actorSys, CodecWrapper[K](ctK, reg), CodecWrapper[V](ctV, reg))
      .withBootstrapServers(serverAndPort)
      .withGroupId(defaultGroupId)
      .withProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(10.seconds)
  }

  def prodSettings[K, V](
      implicit ctK: ClassTag[K],
      ctV: ClassTag[V],
      actorSys: ActorSystem
  ): ProducerSettings[K, V] = {

    ProducerSettings
      .create(actorSys, CodecWrapper[K](ctK, reg), CodecWrapper[V](ctV, reg))
      .withBootstrapServers(serverAndPort)
  }

  override def createEnvironment(): KafkaExecutorEnvironment = {
    new KafkaExecutorEnvironment {

      override val settings: KafkaExecutorSettings = BsonKafkaExecutorSettings.this

      override val context: ExecutionContext = ExecutionContext.global
      implicit override val actors: ActorSystem = ActorSystem(s"ActorSys")
      override val materializer: Materializer = ActorMaterializer.create(actors)

      // Kafka - PiiId keyed consumer topic settings
      override val csPiiHistory: ConsumerSettings[KeyPiiId, PiiHistory] = consSettings
      override val csSequenceRequest: ConsumerSettings[KeyPiiId, SequenceRequest] = consSettings
      override val csReduceRequest: ConsumerSettings[KeyPiiId, ReduceRequest] = consSettings

      // Jev, new results listeners only care about messages after their instantiation.
      // Additionally, they cannot fall back on the old offset as they have a unique group-id.
      override val csResult: ConsumerSettings[KeyPiiId, PiiLog] = {
        consSettings[KeyPiiId, PiiLog].withProperty(AUTO_OFFSET_RESET_CONFIG, "latest")
      }

      // Kafka - (PiiId, CallRef) keyed consumer topic settings
      override val csAssignment: ConsumerSettings[KeyPiiIdCall, Assignment] = consSettings

      // Kafka - All producer settings
      override val psAllMessages: ProducerSettings[AnyKey, AnyMsg] = prodSettings
    }
  }

  override def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg] = {
    case m: PiiUpdate => new ProducerRecord(tnPiiHistory, KeyPiiId(m.pii.id), m)
    case m: SequenceRequest => new ProducerRecord(tnPiiHistory, KeyPiiId(m.piiId), m)
    case m: SequenceFailure => new ProducerRecord(tnPiiHistory, KeyPiiId(m.piiId), m)
    case m: ReduceRequest => new ProducerRecord(tnReduceRequest, KeyPiiId(m.pii.id), m)
    case m: Assignment => new ProducerRecord(tnAssignment, KeyPiiIdCall(m.pii.id, m.callRef), m)
    case m: PiiLog => new ProducerRecord(tnResult, KeyPiiId(m.piiId), m)
  }

}
