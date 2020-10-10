package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.StatelessMessages.PiiUpdate
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs._
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.types.ObjectId
import org.bson.{ BsonReader, BsonWriter }

class PiiUpdateCodec(piiCodec: Codec[PiiT]) extends ClassCodec[PiiUpdate] {

  val piiN = "pii"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiiUpdate = {

    reader.readName(piiN)
    val piiVal: PiInstance[ObjectId] = ctx.decodeWithChildContext(piiCodec, reader)

    PiiUpdate(piiVal)
  }

  override def encodeBody(writer: BsonWriter, value: PiiUpdate, ctx: EncoderContext): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(piiCodec, writer, value.pii)
  }
}
