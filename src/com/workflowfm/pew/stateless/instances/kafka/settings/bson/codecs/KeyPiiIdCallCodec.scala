package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaConnectors.KeyPiiIdCall
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId

class KeyPiiIdCallCodec( refCodec: Codec[CallRef] )
  extends Codec[KeyPiiIdCall] {

  import PewCodecs._

  val idN = "_id"
  val refN = "ref"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPiiIdCall = {
    reader.readStartDocument()

    reader.readName(idN)
    val piiId = reader.readObjectId()

    reader.readName(refN)
    val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

    reader.readEndDocument()
    ( piiId, ref )
  }

  override def encode(writer: BsonWriter, value: KeyPiiIdCall, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    val (piiId, ref) = value

    writer.writeName(idN)
    writer.writeObjectId( piiId )

    writer.writeName(refN)
    ctx.encodeWithChildContext( refCodec, writer, ref )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPiiIdCall] = KEY_PII_ID_CALL
}
