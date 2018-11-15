package com.workflowfm.pew.mongodb.bson.pitypes

import com.workflowfm.pew.{PiTimeType, PiTimes}
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{DecoderContext, EncoderContext}

class PiTimesCodec extends ClassCodec[PiTimes] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val timesN: String = "times"
  val typeN: String = "ttype"
  val valN: String = "tval"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiTimes = {
    val times = readArray( reader, timesN ) {
      () =>
        val timeTypeName: String = reader.readString( typeN )
        val timeType: PiTimeType = Class.forName( timeTypeName ).asInstanceOf

        val timeValue: Long = reader.readInt64( valN )

        timeType -> timeValue
    }

    PiTimes( times: _* )
  }

  override def encodeBody(writer: BsonWriter, value: PiTimes, ctx: EncoderContext): Unit = {
    writeArray( writer, timesN, value.to ) {
      case (timeType, timeValue) =>
        writer.writeString( typeN, timeType.getClass.getName )
        writer.writeInt64( valN, timeValue )
    }
  }

}
