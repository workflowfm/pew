package com.workflowfm.pew.mongodb.bson.events

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.{ PiEventReturn }
import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiEventReturnCodec[T](tCodec: Codec[T], anyCodec: Codec[Any], metaCodec: Codec[PiMetadataMap])
    extends ClassCodec[PiEventReturn[T]] {

  val piiN: String = "pii"
  val refN: String = "ref"
  val resN: String = "res"
  val timeN: String = "timestamp"

  override def encodeBody(
      writer: BsonWriter,
      value: PiEventReturn[T],
      ctx: EncoderContext
  ): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(tCodec, writer, value.id)

    writer.writeInt32(refN, value.ref)

    writer.writeName(resN)
    ctx.encodeWithChildContext(anyCodec, writer, value.result)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventReturn[T] = {

    reader.readName(piiN)
    val id: T = ctx.decodeWithChildContext(tCodec, reader)

    val ref: Int = reader.readInt32(refN)

    reader.readName(resN)
    val result: Any = ctx.decodeWithChildContext(anyCodec, reader)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiEventReturn(id, ref, result, data)
  }
}
