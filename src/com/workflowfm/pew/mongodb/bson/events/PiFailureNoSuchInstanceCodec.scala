package com.workflowfm.pew.mongodb.bson.events

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.{ PiFailureNoSuchInstance }
import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiFailureNoSuchInstanceCodec[T](tCodec: Codec[T], metaCodec: Codec[PiMetadataMap])
    extends ClassCodec[PiFailureNoSuchInstance[T]] {

  val tIdN: String = "tId"
  val timeN: String = "timestamp"

  override def encodeBody(
      writer: BsonWriter,
      value: PiFailureNoSuchInstance[T],
      ctx: EncoderContext
  ): Unit = {

    writer.writeName(tIdN)
    ctx.encodeWithChildContext(tCodec, writer, value.id)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureNoSuchInstance[T] = {

    reader.readName(tIdN)
    val tId: T = ctx.decodeWithChildContext(tCodec, reader)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiFailureNoSuchInstance(tId, data)
  }
}
