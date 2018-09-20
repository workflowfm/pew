package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KeyPii
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.PiiT
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId

class KeyPiiCodec( val piiCodec: Codec[PiiT] )
  extends Codec[KeyPii] {

  import PewCodecs._

  val piiN = "pii"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPii = {
    reader.readStartDocument()

    reader.readName( piiN )
    val pii: PiInstance[ObjectId] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readEndDocument()
    pii
  }

  override def encode(writer: BsonWriter, pii: KeyPii, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, pii )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPii] = KEY_PII
}
