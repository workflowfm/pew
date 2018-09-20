package com.workflowfm.pew.stateless.instances.kafka

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConsumerComponent
import org.apache.kafka.clients.producer.ProducerRecord
import org.bson.types.ObjectId


// Kafka `at-least-once` delivery
// https://dtrung.com/2016/09/14/kafka-gotchas/

object KafkaTopic {

  import com.workflowfm.pew.stateless.StatelessMessages._

  type TopicN = String

  // Kafka - Topic Names
  val tnReduceRequest: TopicN = "ReduceRequest"
  val tnPiiHistory: TopicN = "PiiHistory"
  val tnAssignment: TopicN = "Assignment"
  val tnResult: TopicN = "Result"

  // Kafka - Topic Keys
  type KeyPiiId = ObjectId
  type KeyPiiIdCall = (ObjectId, CallRef)

  type KeyPii = PiInstance[ObjectId]
  type KeyPiiCall = (PiInstance[ObjectId], CallRef)

  // Kafka Stateless Components
  type KafkaSequencerComponent    = KafkaConsumerComponent[KeyPiiId, AnyMsg, AnyKey, AnyMsg]
  type KafkaReducerComponent      = KafkaConsumerComponent[KeyPiiId, ReduceRequest, AnyKey, AnyMsg]
  type KafkaProcExecutorComponent = KafkaConsumerComponent[KeyPiiIdCall, Assignment, AnyKey, AnyMsg]
  type KafkaEventHandlerComponent = KafkaConsumerComponent[KeyPiiId, Result[AnyRes], AnyKey, AnyMsg]

  type AnyKey = Any
  type AnyMsg = Any
  type AnyRes = Any

  case class InvalidProducerMessage[T]( msg: T )
    extends Exception( "INVALID_MSG '" + msg.getClass.toString + "': " + msg.toString )

  private def toMsg( topicName: TopicN, key: AnyKey, value: AnyMsg): ProducerRecord[AnyKey, AnyMsg] = {
    new ProducerRecord( topicName, key, value )
  }

  def toProducerMessage: PartialFunction[AnyMsg, ProducerRecord[AnyKey, AnyMsg]] = {
    case m: PiiUpdate           => toMsg( tnPiiHistory, m.pii.id, m )
    case m: SequenceRequest     => toMsg( tnPiiHistory, m.piiId, m )
    case m: ReduceRequest       => toMsg( tnReduceRequest, m.pii.id, m )

    case m: Assignment          => toMsg( tnAssignment, (m.pii.id, m.callRef), m )
    case m: Result[_]           => toMsg( tnResult, m.pii.id, m )

    case m                      => throw InvalidProducerMessage( m )
  }
}
