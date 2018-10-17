package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventProcessException
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventProcessExceptionCodec[T]( tCodec: Codec[T] )
  extends ClassCodec[PiEventProcessException[T]] {

  val tIdN: String = "tId"
  val callRefN: String = "ref"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"

  override def encodeBody(writer: BsonWriter, value: PiEventProcessException[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( callRefN, value.ref )
    writer.writeString( messageN, value.message )
    writer.writeString( stackTraceN, value.stackTrace )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventProcessException[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( callRefN )
    val msg: String = reader.readString( messageN )
    val trace: String = reader.readString( stackTraceN )

    PiEventProcessException( tId, ref, msg, trace )
  }
}
