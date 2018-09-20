package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KeyPiiId
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class KeyPiiIdCodec
  extends Codec[KeyPiiId] {

  import PewCodecs._

  val idN = "_id"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPiiId = {
    reader.readStartDocument()

    reader.readName( idN )
    val piiId = reader.readObjectId()

    reader.readEndDocument()
    piiId
  }

  override def encode(writer: BsonWriter, piiId: KeyPiiId, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( idN )
    writer.writeObjectId( piiId )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPiiId] = KEY_PII_ID
}
