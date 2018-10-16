package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventProcessException
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventProcessExceptionCodec[T]( tCodec: Codec[T] )
  extends Codec[PiEventProcessException[T]] {

  val tIdN: String = "tId"
  val callRefN: String = "ref"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"

  override def encode(writer: BsonWriter, value: PiEventProcessException[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( callRefN, value.ref )
    writer.writeString( messageN, value.message )
    writer.writeString( stackTraceN, value.stackTrace )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventProcessException[T] = {
    reader.readStartDocument()

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( callRefN )
    val msg: String = reader.readString( messageN )
    val trace: String = reader.readString( stackTraceN )

    reader.readEndDocument()
    PiEventProcessException( tId, ref, msg, trace )
  }

  override def getEncoderClass: Class[PiEventProcessException[T]]
    = classOf[PiEventProcessException[T]]
}
