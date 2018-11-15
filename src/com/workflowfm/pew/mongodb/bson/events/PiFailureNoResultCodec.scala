package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureNoResult, PiInstance, PiTimes}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoResultCodec[T]( piiCodec: Codec[PiInstance[T]], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiFailureNoResult[T]] {

  val piiN: String = "pii"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiFailureNoResult[T], ctx: EncoderContext): Unit = {
    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureNoResult[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiFailureNoResult( pii, time )
  }
}
