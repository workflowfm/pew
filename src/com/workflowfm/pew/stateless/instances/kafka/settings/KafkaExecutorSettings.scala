package com.workflowfm.pew.stateless.instances.kafka.settings

import akka.actor._
import akka.kafka._
import akka.stream._
import com.workflowfm.pew.PiProcessStore
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonCodecWrapper, KafkaCodecProvider}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.bson.types.ObjectId

import scala.concurrent.ExecutionContext

object KafkaExecutorSettings {

  // Kafka - Topic Keys
  type KeyPiiId = ObjectId
  type KeyPiiIdCall = (ObjectId, CallRef)

  type AnyKey = Any
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
  *
  */
class KafkaExecutorSettings(
    processStore: PiProcessStore,
    implicit val actorSys: ActorSystem,
    val execCtx: ExecutionContext = ExecutionContext.global
  ) {

  import KafkaExecutorSettings._
  import com.workflowfm.pew.stateless.StatelessMessages._

  type TopicN = String

  // Kafka - Topic Names
  val tnReduceRequest: TopicN = "ReduceRequest"
  val tnPiiHistory: TopicN = "PiiHistory"
  val tnAssignment: TopicN = "Assignment"
  val tnResult: TopicN = "Result"

  implicit val materializer: Materializer = ActorMaterializer.create( actorSys )

  val serverAndPort: String = "localhost:9092"
  val defaultGroupId: String = "Default-Group"

  implicit val pro: KafkaCodecProvider = new KafkaCodecProvider( processStore )

  // Json deserializers for key types.
  val dsKeyPiiId: Deserializer[KeyPiiId]         = new BsonCodecWrapper( classOf[KeyPiiId] )
  val dsKeyPiiIdCall: Deserializer[KeyPiiIdCall] = new BsonCodecWrapper( classOf[KeyPiiIdCall] )

  // Json deserializers for message objects.
  val dsAssignment: Deserializer[Assignment]  = new BsonCodecWrapper( classOf[Assignment] )
  val dsResult: Deserializer[PiiResult[AnyRes]]  = new BsonCodecWrapper( classOf[PiiResult[AnyRes]] )
  val dsPiiHistory: Deserializer[PiiHistory]      = new BsonCodecWrapper( classOf[PiiHistory] )
  val dsReduceRequest: Deserializer[ReduceRequest] = new BsonCodecWrapper( classOf[ReduceRequest] )
  val dsSequenceRequest: Deserializer[SequenceRequest] = new BsonCodecWrapper( classOf[SequenceRequest] )

  // Json serializers for key and message objects.
  val szKeyPiiId: Serializer[KeyPiiId] = new BsonCodecWrapper( classOf[KeyPiiId] )
  val szKeyPiiIdCall: Serializer[KeyPiiIdCall] = new BsonCodecWrapper( classOf[KeyPiiIdCall] )
  val szAnyKey: Serializer[AnyKey] = new BsonCodecWrapper( pro.anykey )
  val szAnyMsg: Serializer[AnyMsg] = new BsonCodecWrapper( pro.anymsg )

  def consSettings[K, V]( dsKey: Deserializer[K], dsVal: Deserializer[V] )
    : ConsumerSettings[K, V] = {

    import ConsumerConfig._

    ConsumerSettings
      .create( actorSys, dsKey, dsVal )
      .withBootstrapServers( serverAndPort )
      .withGroupId( defaultGroupId )
      .withProperty( AUTO_OFFSET_RESET_CONFIG, "earliest" )
  }

  def prodSettings[K, V]( szKey: Serializer[K], szVal: Serializer[V] )
    : ProducerSettings[K, V] = {

    ProducerSettings
      .create( actorSys, szKey, szVal )
      .withBootstrapServers( serverAndPort )
  }

  // Kafka - PiiId keyed consumer topic settings
  val csPiiHistory:       ConsumerSettings[KeyPiiId, PiiHistory] = consSettings( dsKeyPiiId, dsPiiHistory )
  val csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest] = consSettings( dsKeyPiiId, dsSequenceRequest )
  val csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest] = consSettings( dsKeyPiiId, dsReduceRequest )
  val csResult:           ConsumerSettings[KeyPiiId, PiiResult[AnyRes]] = consSettings( dsKeyPiiId, dsResult )

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment:      ConsumerSettings[KeyPiiIdCall, Assignment] = consSettings( dsKeyPiiIdCall, dsAssignment )

  // Kafka - All producer settings
  val psAllMessages: ProducerSettings[AnyKey, AnyMsg] = prodSettings( szAnyKey, szAnyMsg )

  def record: AnyMsg => ProducerRecord[AnyKey, AnyMsg] = {

    case m: PiiUpdate           => new ProducerRecord( tnPiiHistory, m.pii.id, m )
    case m: SequenceRequest     => new ProducerRecord( tnPiiHistory, m.piiId, m )
    case m: ReduceRequest       => new ProducerRecord( tnReduceRequest, m.pii.id, m )

    case m: Assignment          => new ProducerRecord( tnAssignment, (m.pii.id, m.callRef), m )
    case m: PiiResult[_]        => new ProducerRecord( tnResult, m.pii.id, m )

  }
}
