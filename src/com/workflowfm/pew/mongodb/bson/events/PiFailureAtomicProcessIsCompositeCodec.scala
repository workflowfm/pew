package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiFailureAtomicProcessIsComposite, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureAtomicProcessIsCompositeCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends Codec[PiFailureAtomicProcessIsComposite[T]] {

  val piiIdN: String = "piiId"
  val procN: String = "proc"

  override def encode(writer: BsonWriter, value: PiFailureAtomicProcessIsComposite[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiIdN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( procN, value.process )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiFailureAtomicProcessIsComposite[T] = {
    reader.readStartDocument()

    reader.readName( piiIdN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( procN )

    reader.readEndDocument()
    PiFailureAtomicProcessIsComposite( pii, proc )
  }

  override def getEncoderClass: Class[PiFailureAtomicProcessIsComposite[T]]
    = classOf[PiFailureAtomicProcessIsComposite[T]]
}