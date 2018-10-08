package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiObject
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.AnyRes
import org.bson.codecs._
import org.bson.{BsonReader, BsonWriter}

class AnyResCodec( objCodec: Codec[PiObject], throwCodec: Codec[Throwable] )
  extends Codec[AnyRes] {

  import PewCodecs._

  val piObjN = "piobj"
  val throwableN = "failure"

  override def encode(writer: BsonWriter, value: AnyRes, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    value match {
      case piObj: PiObject =>
        writer.writeName( piObjN )
        ctx.encodeWithChildContext( objCodec, writer, piObj )

      case fail: Throwable =>
        writer.writeName( throwableN )
        ctx.encodeWithChildContext( throwCodec, writer, fail )
    }

    writer.writeEndDocument()
  }

  case class KafkaRemoteException( msg: String )
    extends Exception( msg )

  override def decode(reader: BsonReader, ctx: DecoderContext): AnyRes = {
    reader.readStartDocument()

    val typeN = reader.readName()

    val result: AnyRes =
      if ( typeN == piObjN )
        ctx.decodeWithChildContext( objCodec, reader )

      else if ( typeN == throwableN )
        ctx.decodeWithChildContext( throwCodec, reader )

      else
        KafkaRemoteException( "Unsupported result type (Receiver): " + typeN )

    reader.readEndDocument()
    result
  }

  override def getEncoderClass: Class[AnyRes] = ANY_RES
}
