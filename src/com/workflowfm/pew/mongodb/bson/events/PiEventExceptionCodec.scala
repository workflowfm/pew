package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventException
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventExceptionCodec[T]( tCodec: Codec[T] )
  extends ClassCodec[PiEventException[T]] {

  val tIdN: String = "tId"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"

  override def encodeBody(writer: BsonWriter, value: PiEventException[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeString( messageN, value.message )
    writer.writeString( stackTraceN, value.stackTrace )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventException[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val msg: String = reader.readString( messageN )
    val trace: String = reader.readString( stackTraceN )

    PiEventException( tId, msg, trace )
  }
}