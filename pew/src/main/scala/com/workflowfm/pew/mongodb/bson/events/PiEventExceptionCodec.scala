package com.workflowfm.pew.mongodb.bson.events

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.{ PiFailureExceptions }
import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiEventExceptionCodec[T](tCodec: Codec[T], metaCodec: Codec[PiMetadataMap])
    extends ClassCodec[PiFailureExceptions[T]] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val tIdN: String = "tId"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"
  val timeN: String = "timestamp"

  override def encodeBody(
      writer: BsonWriter,
      value: PiFailureExceptions[T],
      ctx: EncoderContext
  ): Unit = {

    writer.writeName(tIdN)
    ctx.encodeWithChildContext(tCodec, writer, value.id)

    writer.writeString(messageN, value.message)
    writeObjectSeq(writer, stackTraceN, value.trace.toSeq)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureExceptions[T] = {

    reader.readName(tIdN)
    val tId: T = ctx.decodeWithChildContext(tCodec, reader)

    val msg: String = reader.readString(messageN)
    val trace: Array[StackTraceElement] = readObjectSeq(reader, stackTraceN)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiFailureExceptions(tId, msg, trace, data)
  }
}
