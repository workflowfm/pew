package com.workflowfm.pew.mongodb.bson.pitypes

import com.workflowfm.pew.{PiProcess, PiProcessStore}
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiProcessCodec(store: PiProcessStore) extends Codec[PiProcess] {

  val processNameN = "name"

  def getPiProcess(name: String): PiProcess = {
    store.get(name) match {
      case None       => throw new CodecConfigurationException(s"Unknown PiProcess: $name")
      case Some(proc) => proc
    }
  }

  override def encode(writer: BsonWriter, value: PiProcess, ctx: EncoderContext): Unit = {
    val processName: String = value.iname
    require(value == getPiProcess(processName), s"PiProcess '$processName' couldn't be recovered.")

    writer.writeStartDocument()
    writer.writeString(processNameN, processName)
    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiProcess = {
    reader.readStartDocument()
    val processName: String = reader.readString(processNameN)
    reader.readEndDocument()

    getPiProcess(processName)
  }

  override def getEncoderClass: Class[PiProcess] = classOf[PiProcess]
}
