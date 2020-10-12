package com.workflowfm.pew.mongodb.bson.events

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.{ PiFailureUnknownProcess, PiInstance }
import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiFailureUnknownProcessCodec[T](
    piiCodec: Codec[PiInstance[T]],
    metaCodec: Codec[PiMetadataMap]
) extends ClassCodec[PiFailureUnknownProcess[T]] {

  val piiN: String = "pii"
  val atomicProcN: String = "proc"
  val timeN: String = "timestamp"

  override def encodeBody(
      writer: BsonWriter,
      value: PiFailureUnknownProcess[T],
      ctx: EncoderContext
  ): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(piiCodec, writer, value.i)

    writer.writeString(atomicProcN, value.process)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureUnknownProcess[T] = {

    reader.readName(piiN)
    val pii: PiInstance[T] = ctx.decodeWithChildContext(piiCodec, reader)

    val proc: String = reader.readString(atomicProcN)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiFailureUnknownProcess(pii, proc, data)
  }
}
