package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureUnknownProcess, PiInstance, PiTimes}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureUnknownProcessCodec[T]( piiCodec: Codec[PiInstance[T]], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiFailureUnknownProcess[T]]{

  val piiN: String = "pii"
  val atomicProcN: String = "proc"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiFailureUnknownProcess[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( atomicProcN, value.process )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureUnknownProcess[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( atomicProcN )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiFailureUnknownProcess( pii, proc, time )
  }
}
