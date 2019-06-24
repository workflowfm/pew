package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.{PiFailureAtomicProcessException}
import com.workflowfm.pew.mongodb.bson.BsonUtil.{readObjectSeq, writeObjectSeq}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventProcessExceptionCodec[T](tCodec: Codec[T], metaCodec: Codec[PiMetadataMap])
    extends ClassCodec[PiFailureAtomicProcessException[T]] {

  val tIdN: String        = "tId"
  val callRefN: String    = "ref"
  val messageN: String    = "msg"
  val stackTraceN: String = "trace"
  val timeN: String       = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiFailureAtomicProcessException[T], ctx: EncoderContext): Unit = {

    writer.writeName(tIdN)
    ctx.encodeWithChildContext(tCodec, writer, value.id)

    writer.writeInt32(callRefN, value.ref)
    writer.writeString(messageN, value.message)
    writeObjectSeq(writer, stackTraceN, value.trace.toSeq)

    writer.writeName(timeN)
    ctx.encodeWithChildContext(metaCodec, writer, value.metadata)
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureAtomicProcessException[T] = {

    reader.readName(tIdN)
    val tId: T = ctx.decodeWithChildContext(tCodec, reader)

    val ref: Int                        = reader.readInt32(callRefN)
    val msg: String                     = reader.readString(messageN)
    val trace: Array[StackTraceElement] = readObjectSeq(reader, stackTraceN)

    reader.readName(timeN)
    val data: PiMetadataMap = ctx.decodeWithChildContext(metaCodec, reader)

    PiFailureAtomicProcessException(tId, ref, msg, trace, data)
  }
}
