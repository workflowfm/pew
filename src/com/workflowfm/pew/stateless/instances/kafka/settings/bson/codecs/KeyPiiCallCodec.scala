package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.KeyPiiCall
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.PiiT
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId

class KeyPiiCallCodec( piiCodec: Codec[PiiT], refCodec: Codec[CallRef] )
  extends Codec[KeyPiiCall] {

  import PewCodecs._

  val piiN = "pii"
  val refN = "ref"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPiiCall = {
    reader.readStartDocument()

    reader.readName(piiN)
    val pii: PiInstance[ObjectId] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName(refN)
    val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

    reader.readEndDocument()
    ( pii, ref )
  }

  override def encode(writer: BsonWriter, value: KeyPiiCall, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    val (pii, ref) = value

    writer.writeName(piiN)
    ctx.encodeWithChildContext( piiCodec, writer, pii )

    writer.writeName(refN)
    ctx.encodeWithChildContext( refCodec, writer, ref )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPiiCall] = KEY_PII_CALL
}
