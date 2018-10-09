package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.ReduceRequest
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.PiiT
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId
import org.bson.{BsonReader, BsonWriter}

class ReduceRequestCodec( piiCodec: Codec[PiiT], refCodec: Codec[CallRef], objCodec: Codec[PiObject] )
  extends Codec[ReduceRequest] {

  import PewCodecs._

  val msgTypeN = "msgType"
  val msgType = "ReduceRequest"

  val piiN = "pii"
  val argsN = "args"
  val refN = "ref"
  val objN = "obj"

  override def decode(reader: BsonReader, ctx: DecoderContext): ReduceRequest = {
    reader.readStartDocument()
    reader.readString( msgTypeN )

    reader.readName( piiN )
    val pii: PiInstance[ObjectId] = ctx.decodeWithChildContext( piiCodec, reader )

    val args = readArray( reader, argsN ) { () =>
      reader.readStartDocument()

      reader.readName( refN )
      val ref = ctx.decodeWithChildContext( refCodec, reader )

      reader.readName( objN )
      val obj = ctx.decodeWithChildContext( objCodec, reader )

      reader.readEndDocument()
      (ref, obj)
    }

    reader.readEndDocument()
    ReduceRequest( pii, args )
  }

  override def encode(writer: BsonWriter, value: ReduceRequest, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeString( msgTypeN, msgType )

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.pii )

    writeArray( writer, argsN, value.args ) {
      case (ref, obj) =>
        writer.writeStartDocument()

        writer.writeName( refN )
        ctx.encodeWithChildContext( refCodec, writer, ref )

        writer.writeName( objN )
        ctx.encodeWithChildContext( objCodec, writer, obj )

        writer.writeEndDocument()
    }

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[ReduceRequest] = REDUCE_REQUEST

}