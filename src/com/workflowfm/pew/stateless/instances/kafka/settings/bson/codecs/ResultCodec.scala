package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.PiiT
import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.AnyRes
import org.bson._
import org.bson.codecs._

class ResultCodec(
     piiCodec: Codec[PiiT],
     resCodec: Codec[AnyRes],
     refCodec: Codec[CallRef]

  ) extends Codec[PewCodecs.ResMsgT] {

  import PewCodecs._
  import KafkaConnectors._
  import StatelessMessages._

  val piiN = "pii"
  val hasRefN = "hasRef"
  val refN = "ref"
  val resN = "res"

  override def decode(reader: BsonReader, ctx: DecoderContext): ResMsgT = {
    reader.readStartDocument()

    reader.readName( piiN )
    val pii = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName( hasRefN )
    val optRef =
      if ( reader.readBoolean() ) {
        reader.readName( refN )
        Some( ctx.decodeWithChildContext( refCodec, reader ) )

      } else None

    reader.readName( resN )
    val res: AnyRes = ctx.decodeWithChildContext( resCodec, reader )

    reader.readEndDocument()
    PiiResult( pii, optRef, res )
  }

  override def encode(writer: BsonWriter, value: ResMsgT, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.pii )

    writer.writeName( hasRefN )
    value.callRef match {
      case Some( ref ) =>
        writer.writeBoolean( true )

        writer.writeName( refN )
        ctx.encodeWithChildContext( refCodec, writer, ref )

      case None =>
        writer.writeBoolean( false )
    }

    writer.writeName( resN )
    ctx.encodeWithChildContext( resCodec, writer, value.res )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[ResMsgT] = RESULT_ANY_MSG

}