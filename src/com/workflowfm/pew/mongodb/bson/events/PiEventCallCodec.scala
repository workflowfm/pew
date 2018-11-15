package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventCallCodec[T](
    tCodec: Codec[T],
    objCodec: Codec[PiObject],
    procCodec: Codec[PiProcess],
    timeCodec: Codec[PiTimes]

  ) extends ClassCodec[PiEventCall[T]] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val piiIdN: String = "piid"
  val callRefN: String = "ref"
  val atomicProcN: String = "proc"
  val argsN: String = "args"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventCall[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( callRefN, value.ref )

    writer.writeName( atomicProcN )
    ctx.encodeWithChildContext( procCodec, writer, value.p )

    writeArray( writer, argsN, value.args ) {
      ctx.encodeWithChildContext( objCodec, writer, _ )
    }

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventCall[T] = {

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

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )

    PiEventCall( tId, ref, proc, args, time )
  }
}
