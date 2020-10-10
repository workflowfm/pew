package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.PiResource
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.Assignment
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.{ PiResT, PiiT }
import org.bson._
import org.bson.codecs._

class AssignmentCodec(
    piiCodec: Codec[PiiT],
    callRefCodec: Codec[CallRef],
    piResCodec: Codec[PiResT]
) extends ClassCodec[Assignment] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val uuidN = "uuid"
  val piiN = "pii"
  val refN = "ref"
  val procN = "proc"
  val argsN = "args"
  val resN = "res"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Assignment = {

    reader.readName(piiN)
    val pii = ctx.decodeWithChildContext(piiCodec, reader)

    reader.readName(refN)
    val callRef: CallRef = ctx.decodeWithChildContext(callRefCodec, reader)

    reader.readName(procN)
    val proc = reader.readString()

    val args: List[PiResource] = readArray(reader, argsN) { () =>
      reader.readStartDocument()

      reader.readName(refN)
      val arg = ctx.decodeWithChildContext(piResCodec, reader)

      reader.readEndDocument()
      arg
    }

    Assignment(pii, callRef, proc, args)
  }

  override def encodeBody(writer: BsonWriter, value: Assignment, ctx: EncoderContext): Unit = {

    writer.writeName(piiN)
    ctx.encodeWithChildContext(piiCodec, writer, value.pii)

    writer.writeName(refN)
    ctx.encodeWithChildContext(callRefCodec, writer, value.callRef)

    writer.writeName(procN)
    writer.writeString(value.process)

    writeArray(writer, argsN, value.args) { arg =>
      writer.writeStartDocument()

      writer.writeName(refN)
      ctx.encodeWithChildContext(piResCodec, writer, arg)

      writer.writeEndDocument()
    }
  }
}
