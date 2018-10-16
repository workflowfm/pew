package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventException
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventExceptionCodec[T]( tCodec: Codec[T] )
  extends Codec[PiEventException[T]] {

  val tIdN: String = "tId"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"

  override def encode(writer: BsonWriter, value: PiEventException[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeString( messageN, value.message )
    writer.writeString( stackTraceN, value.stackTrace )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventException[T] = {
    reader.readStartDocument()

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val msg: String = reader.readString( messageN )
    val trace: String = reader.readString( stackTraceN )

    reader.readEndDocument()
    PiEventException( tId, msg, trace )
  }

  override def getEncoderClass: Class[PiEventException[T]]
    = classOf[PiEventException[T]]
}