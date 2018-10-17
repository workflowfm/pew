package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.SequenceFailure
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId
import org.bson.{BsonReader, BsonWriter}

class SequenceFailureCodec(
    piiCodec: Codec[PiInstance[ObjectId]],
    refCodec: Codec[CallRef],
    objCodec: Codec[PiObject],
    errCodec: Codec[Throwable]

  ) extends ClassCodec[SequenceFailure] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val hasPiiN = "hasPii"
  val piiN = "pii"
  val resultsN = "results"
  val failuresN = "failures"
  val refN = "ref"
  val objN = "obj"
  val failN = "err"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): SequenceFailure = {

    val hasPii: Boolean = reader.readBoolean( hasPiiN )
    reader.readName( piiN )

    val eitherPii =
      if (hasPii)
        Right( ctx.decodeWithChildContext( piiCodec, reader ) )
      else
        Left( reader.readObjectId() )

    val results = readArray( reader, resultsN ) { () =>
      reader.readStartDocument()

      reader.readName( refN )
      val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

      reader.readName( objN )
      val obj: PiObject = ctx.decodeWithChildContext( objCodec, reader )

      reader.readEndDocument()
      (ref, obj)
    }

    val failures = readArray( reader, failuresN ) { () =>
      reader.readStartDocument()

      reader.readName( refN )
      val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

      reader.readName( failN )
      val fail: Throwable = ctx.decodeWithChildContext( errCodec, reader )

      reader.readEndDocument()
      (ref, fail)
    }

    SequenceFailure(
      eitherPii,
      results,
      failures
    )
  }

  override def encodeBody(writer: BsonWriter, value: SequenceFailure, ctx: EncoderContext): Unit = {

    writer.writeBoolean( hasPiiN, value.pii.isRight )
    writer.writeName( piiN )

    value.pii match {
      case Right( _pii ) => ctx.encodeWithChildContext( piiCodec, writer, _pii )
      case Left( _piiId ) => writer.writeObjectId( _piiId )
    }

    writeArray( writer, resultsN, value.results ) {
      case ( ref, obj ) =>
        writer.writeStartDocument()

        writer.writeName( refN )
        ctx.encodeWithChildContext( refCodec, writer, ref )

        writer.writeName( objN )
        ctx.encodeWithChildContext( objCodec, writer, obj )

        writer.writeEndDocument()
    }

    writeArray( writer, failuresN, value.failures ) {
      case ( ref, fail ) =>
        writer.writeStartDocument()

        writer.writeName( refN )
        ctx.encodeWithChildContext( refCodec, writer, ref )

        writer.writeName( failN )
        ctx.encodeWithChildContext( errCodec, writer, fail )

        writer.writeEndDocument()
    }
  }
}