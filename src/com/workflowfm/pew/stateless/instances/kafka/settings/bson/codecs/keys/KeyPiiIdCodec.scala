package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.keys

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.KeyPiiId
import org.bson.codecs.{ DecoderContext, EncoderContext }
import org.bson.{ BsonReader, BsonWriter }

class KeyPiiIdCodec extends ClassCodec[KeyPiiId] {

  val idN = "id"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): KeyPiiId = {
    reader.readName(idN)
    val piiId = reader.readObjectId()

    KeyPiiId(piiId)
  }

  override def encodeBody(writer: BsonWriter, key: KeyPiiId, ctx: EncoderContext): Unit = {
    writer.writeName(idN)
    writer.writeObjectId(key.piiId)
  }
}
