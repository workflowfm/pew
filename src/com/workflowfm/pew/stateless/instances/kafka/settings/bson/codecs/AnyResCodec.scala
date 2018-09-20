package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiObject
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.AnyRes
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.{BsonReader, BsonWriter}

class AnyResCodec( reg: CodecRegistry )
  extends Codec[AnyRes] {

  import PewCodecs._

  val piObjTN = "piobj"
  val piObjCodec: Codec[PiObject] = reg.get( classOf[PiObject] )

  val throwableTN = "failure"
  // val throwableCodec: Codec[Throwable] = pro.get( classOf[Throwable] )

  override def encode(writer: BsonWriter, value: AnyRes, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()


    value match {
      case piObj: PiObject =>
        writer.writeName( piObjTN )
        ctx.encodeWithChildContext( piObjCodec, writer, piObj )

      case fail: Throwable =>
        writer.writeName( throwableTN )
        writer.writeString( fail.getMessage )

      case _ =>
        writer.writeName( throwableTN )
        writer.writeString( "Unsupported result type (Sender): " + value.getClass.toString )
    }

    writer.writeEndDocument()
  }

  case class KafkaRemoteException( msg: String )
    extends Exception( msg )

  override def decode(reader: BsonReader, ctx: DecoderContext): AnyRes = {
    reader.readStartDocument()

    val typeN = reader.readName()

    val result: AnyRes =
      if ( typeN == piObjTN )
        ctx.decodeWithChildContext( piObjCodec, reader )

      else if ( typeN == throwableTN )
        KafkaRemoteException( reader.readString() )

      else
        KafkaRemoteException( "Unsupported result type (Receiver): " + typeN )

    reader.readEndDocument()
    result
  }

  override def getEncoderClass: Class[AnyRes] = ANY_RES
}
