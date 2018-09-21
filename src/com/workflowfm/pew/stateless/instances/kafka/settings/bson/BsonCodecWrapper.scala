package com.workflowfm.pew.stateless.instances.kafka.settings.bson

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.common.serialization._
import org.bson._
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.io.BasicOutputBuffer

/** A deserializer/serializer wrapper for the MongoDB codec serialization interface
  * so as to use them for Kafka message serialization.
  *
  * @param codec Wrapped codec to leverage for serialization.
  * @tparam T Type that is being serialized.
  */
class BsonCodecWrapper[T]( codec: Codec[T] )
  extends Deserializer[T] with Serializer[T] {

  def this( clazz: Class[T] )( implicit pro: KafkaCodecProvider )
    = this( pro.get[T]( clazz ) )

  private val deCtx: DecoderContext = DecoderContext.builder().build()
  private val enCtx: EncoderContext = EncoderContext.builder().build()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val buffer: ByteBuffer = ByteBuffer.wrap( data )
    val reader: BsonReader = new BsonBinaryReader( buffer )
    codec.decode( reader, deCtx )
  }

  override def serialize(topic: String, data: T): Array[Byte] = {
    val buffer = new BasicOutputBuffer()
    val writer: BsonWriter = new BsonBinaryWriter( buffer )
    codec.encode( writer, data, enCtx )
    buffer.getInternalBuffer
  }

  override def close(): Unit = {}
}
