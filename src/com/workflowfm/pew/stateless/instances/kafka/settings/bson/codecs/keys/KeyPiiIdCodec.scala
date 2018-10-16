package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.keys

import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.KeyPiiId
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class KeyPiiIdCodec
  extends Codec[KeyPiiId] {

  import PewCodecs._

  val idN = "_id"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPiiId = {
    reader.readStartDocument()

    reader.readName( idN )
    val piiId = reader.readObjectId()

    reader.readEndDocument()
    KeyPiiId( piiId )
  }

  override def encode(writer: BsonWriter, key: KeyPiiId, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( idN )
    writer.writeObjectId( key.piiId )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPiiId] = KEY_PII_ID
}
