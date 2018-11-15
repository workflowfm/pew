package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiEventException, PiTimes}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventExceptionCodec[T]( tCodec: Codec[T], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiEventException[T]] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val tIdN: String = "tId"
  val messageN: String = "msg"
  val stackTraceN: String = "trace"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventException[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeString( messageN, value.message )
    writeObjectSeq( writer, stackTraceN, value.trace.toSeq )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventException[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val msg: String = reader.readString( messageN )
    val trace: Array[StackTraceElement] = readObjectSeq( reader, stackTraceN )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiEventException( tId, msg, trace, time )
  }
}