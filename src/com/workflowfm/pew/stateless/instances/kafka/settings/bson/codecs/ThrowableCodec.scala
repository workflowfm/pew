package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class ThrowableCodec
  extends Codec[Throwable] {

  import PewCodecs._

  val msgN: String = "message"

  override def encode(writer: BsonWriter, value: Throwable, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeString( msgN, value.getMessage )
    writer.writeEndDocument()
  }

  case class KafkaRemoteException( msg: String )
    extends Exception( msg )

  override def decode(reader: BsonReader, ctx: DecoderContext): Throwable = {
    reader.readStartDocument()
    val message: String = reader.readString( msgN )
    reader.readEndDocument()
    KafkaRemoteException( message )
  }

  override def getEncoderClass: Class[Throwable] = THROWABLE
}