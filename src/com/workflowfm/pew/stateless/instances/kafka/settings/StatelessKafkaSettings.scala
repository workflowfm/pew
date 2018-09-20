package com.workflowfm.pew.stateless.instances.kafka.settings

import akka.actor._
import akka.kafka._
import akka.stream._
import com.workflowfm.pew.PiProcessStore
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonCodecWrapper, KafkaCodecProvider}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._

import scala.concurrent.ExecutionContext

class StatelessKafkaSettings(
    processStore: PiProcessStore,
    implicit val actorSys: ActorSystem,
    val execCtx: ExecutionContext = ExecutionContext.global
  ) {

  import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic._
  import com.workflowfm.pew.stateless.StatelessMessages._

  implicit val materializer: Materializer = ActorMaterializer.create( actorSys )

  val serverAndPort: String = "localhost:9092"
  val defaultGroupId: String = "Default-Group"

  implicit val pro: KafkaCodecProvider = new KafkaCodecProvider( processStore )

  // Json deserializers for key types.
  val dsKeyPiiId: Deserializer[KeyPiiId]         = new BsonCodecWrapper( classOf[KeyPiiId] )
  val dsKeyPiiIdCall: Deserializer[KeyPiiIdCall] = new BsonCodecWrapper( classOf[KeyPiiIdCall] )

  // Json deserializers for message objects.
  val dsAssignment: Deserializer[Assignment]  = new BsonCodecWrapper( classOf[Assignment] )
  val dsResult: Deserializer[Result[AnyRes]]  = new BsonCodecWrapper( classOf[Result[AnyRes]] )
  val dsPiiHistory: Deserializer[AnyMsg]      = new BsonCodecWrapper( pro.anymsg )
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
  val csPiiHistory:       ConsumerSettings[KeyPiiId, AnyMsg] = consSettings( dsKeyPiiId, dsPiiHistory )
  val csSequenceRequest:  ConsumerSettings[KeyPiiId, SequenceRequest] = consSettings( dsKeyPiiId, dsSequenceRequest )
  val csReduceRequest:    ConsumerSettings[KeyPiiId, ReduceRequest] = consSettings( dsKeyPiiId, dsReduceRequest )
  val csResult:           ConsumerSettings[KeyPiiId, Result[AnyRes]] = consSettings( dsKeyPiiId, dsResult )

  // Kafka - (PiiId, CallRef) keyed consumer topic settings
  val csAssignment:      ConsumerSettings[KeyPiiIdCall, Assignment] = consSettings( dsKeyPiiIdCall, dsAssignment )


  // Kafka - All producer settings
  val psAllMessages: ProducerSettings[AnyKey, AnyMsg] = prodSettings( szAnyKey, szAnyMsg )
}
