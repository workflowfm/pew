package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiFailureNoSuchInstance, PiTimes}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoSuchInstanceCodec[T]( tCodec: Codec[T], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiFailureNoSuchInstance[T]] {

  val tIdN: String = "tId"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiFailureNoSuchInstance[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureNoSuchInstance[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiFailureNoSuchInstance( tId, time )
  }
}
