package com.workflowfm.pew.stateless.instances.kafka.settings

import akka.actor._
import akka.kafka._
import akka.stream._
import com.workflowfm.pew.stateless.{CallRef, StatelessMessages}
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.types.ObjectId

import scala.concurrent.ExecutionContext

object KafkaExecutorSettings {

  // Kafka - Topic Keys
  sealed trait AnyKey
  case class KeyPiiId( piiId: ObjectId ) extends AnyKey
  case class KeyPiiIdCall( piiId: ObjectId, ref: CallRef) extends AnyKey

  type AnyRes = Any

}

/** Low-level Kafka interface:
  * Controls how Kafka Connectors connect to kafka topics, eg:
  * - What the topic names are.
  * - How to connect to the kafka server.
  * - Kafka Producer & Consumer settings.
  * - How to serialize & deserialize messages.
  * - Where to route produced messages.
  *
  * IMPORTANT: Ideally these settings should be the same for *every*
  * Consumer or Producer running on a Kafka cluster, otherwise they
  * might send messages incompatible with other components.
  */
abstract class KafkaExecutorSettings(
    implicit val actorSys: ActorSystem,
    val execCtx: ExecutionContext = ExecutionContext.global
  ) {

  import KafkaExecutorSettings._
  import StatelessMessages._

  type TopicN = String

  // Kafka - Topic Names
  val tnReduceRequest: TopicN
  val tnPiiHistory: TopicN
  val tnAssignment: TopicN
  val tnResult: TopicN

  val mat: Materializer

  val serverAndPort: String
  val defaultGroupId: String

  // Kafka - PiiId keyed consumer topic settings
  val csPiiHistory:       ConsumerSettings[KeyPiiId, PiiHistory]
  val csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest]
  val csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest]
  val csResult:           ConsumerSettings[KeyPiiId, PiiLog]

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment:       ConsumerSettings[KeyPiiIdCall, Assignment]

  // Kafka - All producer settings
  val psAllMessages:      ProducerSettings[AnyKey, AnyMsg]

  def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg]
}
