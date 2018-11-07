package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventProcessException
import com.workflowfm.pew.mongodb.bson.BsonUtil.{readObjectSeq, writeObjectSeq}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventProcessExceptionCodec[T]( tCodec: Codec[T] )
  extends ClassCodec[PiEventProcessException[T]] {

  val tIdN: String = "tId"
  val callRefN: String = "ref"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventProcessException[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( callRefN, value.ref )
    writer.writeString( messageN, value.message )
    writeObjectSeq( writer, stackTraceN, value.trace.toSeq )
    
    writer.writeInt64( timeN, value.time )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventProcessException[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( callRefN )
    val msg: String = reader.readString( messageN )
    val trace: Array[StackTraceElement] = readObjectSeq( reader, stackTraceN )

    val time: Long = reader.readInt64( timeN )
    
    PiEventProcessException( tId, ref, msg, trace, time )
  }
}
