package com.workflowfm.pew.mongodb.bson.pitypes

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.BsonUtil
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{CodecConfigurationException, CodecRegistry}
import org.bson.types._

class PiInstanceCodec(registry: CodecRegistry, processes: PiProcessStore) extends Codec[PiInstance[ObjectId]] {
  val stateCodec: Codec[PiState] = registry.get(classOf[PiState])

  import BsonUtil._

  override def encode(writer: BsonWriter, value: PiInstance[ObjectId], encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeName("_id")
    writer.writeObjectId(value.id)

    writeArray(writer, "calls", value.called) {
      writer.writeInt32
    }

    writer.writeName("process")
    writer.writeString(value.process.iname)

    writer.writeName("state")
    encoderContext.encodeWithChildContext(stateCodec, writer, value.state)

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[PiInstance[ObjectId]] = classOf[PiInstance[ObjectId]]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiInstance[ObjectId] = {
    reader.readStartDocument()
    reader.readName("_id")
    val id = reader.readObjectId()

    val calls: List[Int] = readArray(reader, "calls") { () =>
      reader.readInt32()
    }

    reader.readName("process")
    val iname = reader.readString()
    val p     = processes.getOrElse(iname, throw new CodecConfigurationException("Unknown PiProcess: " + iname))

    reader.readName("state")
    val state = decoderContext.decodeWithChildContext(stateCodec, reader)

    reader.readEndDocument()
    PiInstance(id, calls, p, state)
  }
}
