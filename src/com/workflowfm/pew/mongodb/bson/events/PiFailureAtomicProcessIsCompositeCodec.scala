package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureAtomicProcessIsComposite, PiInstance, PiTimes}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureAtomicProcessIsCompositeCodec[T]( piiCodec: Codec[PiInstance[T]], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiFailureAtomicProcessIsComposite[T]] {

  val piiIdN: String = "piiId"
  val procN: String = "proc"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiFailureAtomicProcessIsComposite[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiIdN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( procN, value.process )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureAtomicProcessIsComposite[T] = {

    reader.readName( piiIdN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( procN )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiFailureAtomicProcessIsComposite( pii, proc, time )
  }
}