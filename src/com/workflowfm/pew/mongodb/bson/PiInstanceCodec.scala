package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue
import org.bson.codecs.configuration.CodecConfigurationException

class PiInstanceCodec(registry:CodecRegistry,processes:Map[String,PiProcess]) extends Codec[PiInstance[ObjectId]] { 
  val stateCodec:Codec[PiState] = registry.get(classOf[PiState])
  
  override def encode(writer: BsonWriter, value: PiInstance[ObjectId], encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
    writer.writeName("_id")
    writer.writeObjectId(value.id)
    
	  writer.writeName("calls")
    writer.writeStartArray()
    for (i <- value.called) writer.writeInt32(i) 
    writer.writeEndArray()

	  writer.writeName("process")
	  writer.writeString(value.process.iname)
	  
	  writer.writeName("state")
    encoderContext.encodeWithChildContext(stateCodec,writer,value.state)

    writer.writeEndDocument()
  }
 
  override def getEncoderClass: Class[PiInstance[ObjectId]] = classOf[PiInstance[ObjectId]]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiInstance[ObjectId] = {
	  reader.readStartDocument()
	  reader.readName("_id")
	  val id = reader.readObjectId()
	  
	  reader.readName("calls")
	  reader.readStartArray()
    var calls:Queue[Int] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      calls += reader.readInt32()
    }
    reader.readEndArray()

    reader.readName("process")
    val iname = reader.readString()
    val p = processes.getOrElse(iname,throw new CodecConfigurationException("Unknown PiProcess: " + iname))
    
    reader.readName("state")
    val state = decoderContext.decodeWithChildContext(stateCodec,reader)
    
	  reader.readEndDocument()
	  PiInstance(id,calls.toSeq,p,state)
  }
}