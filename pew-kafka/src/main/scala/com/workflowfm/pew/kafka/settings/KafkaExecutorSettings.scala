package com.workflowfm.pew.kafka.settings

import scala.concurrent.ExecutionContext

import akka.actor.ActorSystem
import akka.kafka._
import akka.stream.Materializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.types.ObjectId

import com.workflowfm.pew.kafka.settings.KafkaExecutorSettings.{ AnyKey, KeyPiiId, KeyPiiIdCall }
import com.workflowfm.pew.stateless.{ CallRef, StatelessMessages }
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.util.ClassLoaderUtil.withClassLoader

abstract class KafkaExecutorEnvironment {

  val settings: KafkaExecutorSettings

  val context: ExecutionContext
  val actors: ActorSystem
  val materializer: Materializer

  // Kafka - PiiId keyed consumer topic settings
  val csPiiHistory: ConsumerSettings[KeyPiiId, PiiHistory]
  val csSequenceRequest: ConsumerSettings[KeyPiiId, SequenceRequest]
  val csReduceRequest: ConsumerSettings[KeyPiiId, ReduceRequest]
  val csResult: ConsumerSettings[KeyPiiId, PiiLog]

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment: ConsumerSettings[KeyPiiIdCall, Assignment]

  // Kafka - All producer settings
  val psAllMessages: ProducerSettings[AnyKey, AnyMsg]


  /** Create a KafkaProducer from settings whilst explicitly un-setting the ClassLoader.
    * This by-passes errors encountered in the KafkaProducer constructor where Threads
    * within an ExecutionContext do not list the necessary key or value serialiser classes.
    * Explicitly setting `null` causes the constructor to use the Kafka ClassLoader
    * which should contain these values.
    *
    * (Note: Use `lazyProducer` to minimize the number of new Producers which are created,
    * this reduces the number of system resources used (such as file handles))
    * Note on the note: lazyProducer is no longer available in the latest version
    * Note on the note on the note: Reintroducing a custom lazy producer here.
    */
  lazy val producer: org.apache.kafka.clients.producer.Producer[AnyKey, AnyMsg] = 
    withClassLoader(null) {
      psAllMessages.createKafkaProducer()
    }

}

object KafkaExecutorSettings {

  // Kafka - Topic Keys
  sealed trait AnyKey
  case class KeyPiiId(piiId: ObjectId) extends AnyKey
  case class KeyPiiIdCall(piiId: ObjectId, ref: CallRef) extends AnyKey

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
