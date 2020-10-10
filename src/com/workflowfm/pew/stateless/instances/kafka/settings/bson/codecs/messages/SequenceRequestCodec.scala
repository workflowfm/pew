package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.PiObject
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.SequenceRequest
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.{ BsonReader, BsonWriter }

class SequenceRequestCodec(
    refCodec: Codec[CallRef],
    objCodec: Codec[PiObject]
) extends ClassCodec[SequenceRequest] {

  val idN = "_id"
  val refN = "callRef"
  val objN = "piObj"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): SequenceRequest = {

    reader.readName(idN)
    val piiId = reader.readObjectId()

    reader.readName(refN)
    val ref: CallRef = ctx.decodeWithChildContext(refCodec, reader)

    reader.readName(objN)
    val obj: PiObject = ctx.decodeWithChildContext(objCodec, reader)

    SequenceRequest(piiId, (ref, obj))
  }

  override def encodeBody(writer: BsonWriter, value: SequenceRequest, ctx: EncoderContext): Unit = {

    writer.writeName(idN)
    writer.writeObjectId(value.piiId)

    val (callRef, piObj) = value.request

    writer.writeName(refN)
    ctx.encodeWithChildContext(refCodec, writer, callRef)

    writer.writeName(objN)
    ctx.encodeWithChildContext(objCodec, writer, piObj)
  }

}
