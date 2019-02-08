package com.workflowfm.pew.stateless.instances.kafka.settings

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.Materializer
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.{AnyKey, KeyPiiId, KeyPiiIdCall}
import com.workflowfm.pew.stateless.{CallRef, StatelessMessages}
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.types.ObjectId

import scala.concurrent.ExecutionContext

abstract class KafkaExecutorEnvironment {

  val settings: KafkaExecutorSettings

  val context: ExecutionContext
  val actors: ActorSystem
  val materializer: Materializer

  // Kafka - PiiId keyed consumer topic settings
  val csPiiHistory:       ConsumerSettings[KeyPiiId, PiiHistory]
  val csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest]
  val csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest]
  val csResult:           ConsumerSettings[KeyPiiId, PiiLog]

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment:       ConsumerSettings[KeyPiiIdCall, Assignment]

  // Kafka - All producer settings
  val psAllMessages:      ProducerSettings[AnyKey, AnyMsg]

}

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
abstract class KafkaExecutorSettings {

  import KafkaExecutorSettings._
  import StatelessMessages._

  // Kafka - Debug output
  def logMessageReceived(msg: Any): Unit = println(s"Received: $msg.")
  def logMessageSent(msg: Any): Unit = println(s"Sent: $msg.")

  // Kafka - Topic Names
  type TopicN = String
  def tnReduceRequest: TopicN
  def tnPiiHistory: TopicN
  def tnAssignment: TopicN
  def tnResult: TopicN

  def serverAndPort: String
  def defaultGroupId: String

  def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg]

  def createEnvironment(): KafkaExecutorEnvironment

}
