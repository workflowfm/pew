package com.workflowfm.pew.mongodb.bson.events

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.{ PiEventStart, PiInstance }
import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiEventStartCodec[T](piiCodec: Codec[PiInstance[T]], metaCodec: Codec[PiMetadataMap])
    extends ClassCodec[PiEventStart[T]] {

  val piiN: String = "pii"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventStart[T], ctx: EncoderContext): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(piiCodec, writer, value.i)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventStart[T] = {

    reader.readName(piiN)
    val pii: PiInstance[T] = ctx.decodeWithChildContext(piiCodec, reader)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiEventStart(pii, data)
  }
}
