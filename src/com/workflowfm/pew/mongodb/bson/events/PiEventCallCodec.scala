package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew._
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventCallCodec[T]( tCodec: Codec[T], objCodec: Codec[PiObject], procCodec: Codec[PiProcess] )
  extends Codec[PiEventCall[T]] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val piiIdN: String = "piid"
  val callRefN: String = "ref"
  val atomicProcN: String = "proc"
  val argsN: String = "args"

  override def encode(writer: BsonWriter, value: PiEventCall[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( callRefN, value.ref )

    writer.writeName( atomicProcN )
    ctx.encodeWithChildContext( procCodec, writer, value.p )

    writeArray( writer, argsN, value.args ) {
      ctx.encodeWithChildContext( objCodec, writer, _ )
    }

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventCall[T] = {
    reader.readStartDocument()

    reader.readName( piiIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( callRefN )

    val proc: AtomicProcess
      = ctx.decodeWithChildContext( procCodec, reader )
        .asInstanceOf[AtomicProcess]

    val args: Seq[PiObject]
      = readArray( reader, argsN ) { () =>
        ctx.decodeWithChildContext( objCodec, reader )
      }

    reader.readEndDocument()
    PiEventCall( tId, ref, proc, args )
  }

  override def getEncoderClass: Class[PiEventCall[T]]
    = classOf[PiEventCall[T]]
}
