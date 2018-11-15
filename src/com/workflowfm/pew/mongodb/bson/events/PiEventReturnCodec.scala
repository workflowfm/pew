package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiEventReturn, PiTimes}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventReturnCodec[T]( tCodec: Codec[T], anyCodec: Codec[Any], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiEventReturn[T]] {

  val piiN: String = "pii"
  val refN: String = "ref"
  val resN: String = "res"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventReturn[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( refN, value.ref )

    writer.writeName( resN )
    ctx.encodeWithChildContext( anyCodec, writer, value.result )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventReturn[T] = {

    reader.readName( piiN )
    val id: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( refN )

    reader.readName( resN )
    val result: Any = ctx.decodeWithChildContext( anyCodec, reader )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiEventReturn( id, ref, result, time )
  }
}
