package com.workflowfm.pew.kafka.settings.bson.codecs.content

import org.bson._
import org.bson.codecs._

import com.workflowfm.pew.kafka.settings.bson.codecs.PewCodecs
import com.workflowfm.pew.stateless.CallRef

class CallRefCodec extends Codec[CallRef] {

  import PewCodecs._

  val idN = "id"

  override def decode(reader: BsonReader, ctx: DecoderContext): CallRef = {
    reader.readStartDocument()

    reader.readName(idN)
    val id = reader.readInt32()

    reader.readEndDocument()
    CallRef(id)
  }

  override def encode(writer: BsonWriter, value: CallRef, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName(idN)
    writer.writeInt32(value.id)

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[CallRef] = CALL_REF
}
