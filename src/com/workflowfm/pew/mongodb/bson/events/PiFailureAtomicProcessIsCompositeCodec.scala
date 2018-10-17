package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureAtomicProcessIsComposite, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureAtomicProcessIsCompositeCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends ClassCodec[PiFailureAtomicProcessIsComposite[T]] {

  val piiIdN: String = "piiId"
  val procN: String = "proc"

  override def encodeBody(writer: BsonWriter, value: PiFailureAtomicProcessIsComposite[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiIdN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( procN, value.process )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureAtomicProcessIsComposite[T] = {

    reader.readName( piiIdN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( procN )

    PiFailureAtomicProcessIsComposite( pii, proc )
  }
}