package com.workflowfm.pew.kafka.settings.bson.codecs.keys

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.kafka.settings.KafkaExecutorSettings.KeyPiiIdCall
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef

class KeyPiiIdCallCodec(refCodec: Codec[CallRef]) extends ClassCodec[KeyPiiIdCall] {

  val idN = "id"
  val callRefN = "ref"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): KeyPiiIdCall = {

    reader.readName(idN)
    val piiId = reader.readObjectId()

    reader.readName(callRefN)
    val ref: CallRef = ctx.decodeWithChildContext(refCodec, reader)

    KeyPiiIdCall(piiId, ref)
  }

  override def encodeBody(writer: BsonWriter, key: KeyPiiIdCall, ctx: EncoderContext): Unit = {

    writer.writeName(idN)
    writer.writeObjectId(key.piiId)

    writer.writeName(callRefN)
    ctx.encodeWithChildContext(refCodec, writer, key.ref)
  }
}
