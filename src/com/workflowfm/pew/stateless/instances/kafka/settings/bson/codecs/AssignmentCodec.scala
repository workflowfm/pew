package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiResource
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.Assignment
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs.{PiResT, PiiT}
import org.bson._
import org.bson.codecs._

import scala.collection.mutable

class AssignmentCodec(
    piiCodec:     Codec[PiiT],
    callRefCodec: Codec[CallRef],
    piResCodec:   Codec[PiResT]

  ) extends Codec[Assignment] {

  import PewCodecs._

  val msgTypeN = "msgType"
  val msgType = "Assignment"

  val uuidN = "uuid"
  val piiN = "pii"
  val refN = "ref"
  val procN = "proc"
  val argsN = "args"
  val resN = "res"

  override def decode(reader: BsonReader, ctx: DecoderContext): Assignment = {
    reader.readStartDocument()
    reader.readString( msgTypeN )

    reader.readName( piiN )
    val pii = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName( refN )
    val callRef: CallRef = ctx.decodeWithChildContext( callRefCodec, reader )

    reader.readName( procN )
    val proc = reader.readString()

    reader.readName( argsN )
    reader.readStartArray()
    var args: mutable.Queue[PiResource] = mutable.Queue()

    while ( reader.readBsonType() != BsonType.END_OF_DOCUMENT )
      args += {
        reader.readStartDocument()

        reader.readName( refN )
        val arg = ctx.decodeWithChildContext( piResCodec, reader )

        reader.readEndDocument()
        arg
      }

    reader.readEndArray()

    reader.readEndDocument()
    Assignment( pii, callRef, proc, args )
  }

  override def encode(writer: BsonWriter, value: Assignment, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeString( msgTypeN, msgType )

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.pii )

    writer.writeName( refN )
    ctx.encodeWithChildContext( callRefCodec, writer, value.callRef )

    writer.writeName( procN )
    writer.writeString( value.process )

    writer.writeName( argsN )
    writer.writeStartArray()
    for ( res: PiResource <- value.args ) {
      writer.writeStartDocument()

      writer.writeName( refN )
      ctx.encodeWithChildContext( piResCodec, writer, res )

      writer.writeEndDocument()
    }
    writer.writeEndArray()

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[Assignment] = ASSIGNMENT

}