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
    implicit val executionContext: ExecutionContext = ExecutionContext.global
  ) {

  import KafkaExecutorSettings._
  import StatelessMessages._

  type TopicN = String

  // Kafka - Topic Names
  def tnReduceRequest: TopicN
  def tnPiiHistory: TopicN
  def tnAssignment: TopicN
  def tnResult: TopicN

  def mat: Materializer

  def serverAndPort: String
  def defaultGroupId: String

  // Kafka - PiiId keyed consumer topic settings
  def csPiiHistory:       ConsumerSettings[KeyPiiId, PiiHistory]
  def csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest]
  def csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest]
  def csResult:           ConsumerSettings[KeyPiiId, PiiLog]

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  def csAssignment:       ConsumerSettings[KeyPiiIdCall, Assignment]

  // Kafka - All producer settings
  def psAllMessages:      ProducerSettings[AnyKey, AnyMsg]

  def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg]
}
