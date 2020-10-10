package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.ReduceRequest
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs._
import com.workflowfm.pew.{ PiInstance, PiObject }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.types.ObjectId
import org.bson.{ BsonReader, BsonWriter }

class ReduceRequestCodec(piiCodec: Codec[PiiT], refCodec: Codec[CallRef], objCodec: Codec[PiObject])
    extends ClassCodec[ReduceRequest] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val piiN = "pii"
  val argsN = "args"
  val refN = "ref"
  val objN = "obj"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): ReduceRequest = {

    reader.readName(piiN)
    val pii: PiInstance[ObjectId] = ctx.decodeWithChildContext(piiCodec, reader)

    val args = readArray(reader, argsN) { () =>
      reader.readStartDocument()

      reader.readName(refN)
      val ref = ctx.decodeWithChildContext(refCodec, reader)

      reader.readName(objN)
      val obj = ctx.decodeWithChildContext(objCodec, reader)

      reader.readEndDocument()
      (ref, obj)
    }

    ReduceRequest(pii, args)
  }

  override def encodeBody(writer: BsonWriter, value: ReduceRequest, ctx: EncoderContext): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(piiCodec, writer, value.pii)

    writeArray(writer, argsN, value.args) {
      case (ref, obj) =>
        writer.writeStartDocument()

        writer.writeName(refN)
        ctx.encodeWithChildContext(refCodec, writer, ref)

        writer.writeName(objN)
        ctx.encodeWithChildContext(objCodec, writer, obj)

        writer.writeEndDocument()
    }
  }

}
