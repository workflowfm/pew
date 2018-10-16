package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiFailureUnknownProcess, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureUnknownProcessCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends Codec[PiFailureUnknownProcess[T]]{

  val piiN: String = "pii"
  val atomicProcN: String = "proc"

  override def encode(writer: BsonWriter, value: PiFailureUnknownProcess[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( atomicProcN, value.process )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiFailureUnknownProcess[T] = {
    reader.readStartDocument()

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( atomicProcN )

    reader.readEndDocument()
    PiFailureUnknownProcess( pii, proc )
  }

  override def getEncoderClass: Class[PiFailureUnknownProcess[T]]
    = classOf[PiFailureUnknownProcess[T]]
}
