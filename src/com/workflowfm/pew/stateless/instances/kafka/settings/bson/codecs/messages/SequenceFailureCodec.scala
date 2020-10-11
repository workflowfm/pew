package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.types.ObjectId

import com.workflowfm.pew.{ PiFailure, PiInstance, PiObject }
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.SequenceFailure

class SequenceFailureCodec(
    piiCodec: Codec[PiInstance[ObjectId]],
    refCodec: Codec[CallRef],
    objCodec: Codec[PiObject],
    errCodec: Codec[PiFailure[ObjectId]]
) extends ClassCodec[SequenceFailure] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val hasPiiN = "hasPii"
  val piiN = "pii"
  val resultsN = "results"
  val failuresN = "failures"
  val refN = "ref"
  val hasObjN = "hasObj"
  val objN = "obj"
  val failN = "err"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): SequenceFailure = {

    val hasPii: Boolean = reader.readBoolean(hasPiiN)
    reader.readName(piiN)

    val eitherPii =
      if (hasPii)
        Right(ctx.decodeWithChildContext(piiCodec, reader))
      else
        Left(reader.readObjectId())

    val results = readArray(reader, resultsN) { () =>
      reader.readStartDocument()

      reader.readName(refN)
      val ref: CallRef = ctx.decodeWithChildContext(refCodec, reader)

      val hasObj: Boolean = reader.readBoolean(hasObjN)

      val obj: PiObject =
        if (hasObj) {
          reader.readName(objN)
          ctx.decodeWithChildContext(objCodec, reader)
        } else null

      reader.readEndDocument()
      (ref, obj)
    }

    val failures = readArray(reader, failuresN) { () =>
      ctx.decodeWithChildContext(errCodec, reader)
    }

    SequenceFailure(
      eitherPii,
      results,
      failures
    )
  }

  override def encodeBody(writer: BsonWriter, value: SequenceFailure, ctx: EncoderContext): Unit = {

    writer.writeBoolean(hasPiiN, value.pii.isRight)
    writer.writeName(piiN)

    value.pii match {
      case Right(_pii) => ctx.encodeWithChildContext(piiCodec, writer, _pii)
      case Left(_piiId) => writer.writeObjectId(_piiId)
    }

    writeArray(writer, resultsN, value.returns) {
      case (ref, obj) =>
        writer.writeStartDocument()

        writer.writeName(refN)
        ctx.encodeWithChildContext(refCodec, writer, ref)

        writer.writeBoolean(hasObjN, obj != null)

        if (obj != null) {
          writer.writeName(objN)
          ctx.encodeWithChildContext(objCodec, writer, obj)
        }

        writer.writeEndDocument()
    }

    writeArray(writer, failuresN, value.errors) {
      ctx.encodeWithChildContext(errCodec, writer, _)
    }
  }
}
