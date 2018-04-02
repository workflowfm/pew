package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue
import org.bson.codecs.configuration.CodecConfigurationException

class PiStateCodec(registry:CodecRegistry,processes:Map[String,PiProcess]) extends Codec[PiState] { 
  val chanCodec:Codec[Chan] = registry.get(classOf[Chan])
  val termCodec:Codec[Term] = registry.get(classOf[Term])
  val futureCodec:Codec[PiFuture] = registry.get(classOf[PiFuture])
  val cmapCodec:Codec[ChanMap] = registry.get(classOf[ChanMap])
  
  override def encode(writer: BsonWriter, value: PiState, encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
//	  writer.writeName("_t")
//	  writer.writeString("PiState")
	  
    writer.writeName("inputs")
    writer.writeStartArray()
    for ((c,i) <- value.inputs) {
      writer.writeStartDocument()
      writer.writeName("c")
      encoderContext.encodeWithChildContext(chanCodec,writer,c)
      writer.writeName("i")
      encoderContext.encodeWithChildContext(termCodec,writer,i)
      writer.writeEndDocument()
    }
	  writer.writeEndArray()

	  writer.writeName("outputs")
    writer.writeStartArray()
	  for ((c,o) <- value.outputs) {
      writer.writeStartDocument()
      writer.writeName("c")
      encoderContext.encodeWithChildContext(chanCodec,writer,c)
      writer.writeName("o")
      encoderContext.encodeWithChildContext(termCodec,writer,o)
      writer.writeEndDocument()
    }
	  writer.writeEndArray()
 
	  writer.writeName("calls")
    writer.writeStartArray()
	  for (f <- value.calls) encoderContext.encodeWithChildContext(futureCodec,writer,f)
	  writer.writeEndArray()
   
	  writer.writeName("threads")
    writer.writeStartArray()
	  for ((i,t) <- value.threads) {
      writer.writeStartDocument()
      writer.writeName("i")
      writer.writeInt32(i)
      writer.writeName("t")
      encoderContext.encodeWithChildContext(futureCodec,writer,t)
      writer.writeEndDocument()
    }
	  writer.writeEndArray()
	  
	  writer.writeName("tCtr")
	  writer.writeInt32(value.threadCtr)
	  
	  writer.writeName("fCtr")
	  writer.writeInt32(value.freshCtr)
	  
	  writer.writeName("procs")
    writer.writeStartArray()
	  for ((_,p) <- value.processes) writer.writeString(p.iname)
	  writer.writeEndArray()
	  
	  writer.writeName("map")
	  encoderContext.encodeWithChildContext(cmapCodec,writer,value.resources)
	  
	  writer.writeEndDocument()
  }
 
  override def getEncoderClass: Class[PiState] = classOf[PiState]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiState = {
	  reader.readStartDocument()
//	  reader.readName("_t")
//	  reader.readString("PiState")
	  
	  reader.readName("inputs")
	  reader.readStartArray()
    var inputs:Queue[(Chan,Input)] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      reader.readStartDocument()
      reader.readName("c")
      val c = decoderContext.decodeWithChildContext(chanCodec,reader)
      reader.readName("i")
      val i = decoderContext.decodeWithChildContext(termCodec,reader).asInstanceOf[Input] // TODO handle exception
      reader.readEndDocument()
    	inputs+=((c,i))
    }
    reader.readEndArray()

    reader.readName("outputs")
	  reader.readStartArray()
    var outputs:Queue[(Chan,Output)] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      reader.readStartDocument()
      reader.readName("c")
      val c = decoderContext.decodeWithChildContext(chanCodec,reader)
      reader.readName("o")
      val o = decoderContext.decodeWithChildContext(termCodec,reader).asInstanceOf[Output] // TODO handle exception
      reader.readEndDocument()
    	outputs+=((c,o))
    }
    reader.readEndArray()

    reader.readName("calls")
	  reader.readStartArray()
    var calls:Queue[PiFuture] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      val f = decoderContext.decodeWithChildContext(futureCodec,reader)
    	calls+=f
    }
    reader.readEndArray()

    reader.readName("threads")
	  reader.readStartArray()
    var threads:Queue[(Int,PiFuture)] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      reader.readStartDocument()
      reader.readName("i")
      val i = reader.readInt32()
      reader.readName("t")
      val t = decoderContext.decodeWithChildContext(futureCodec,reader)
    	reader.readEndDocument()
      threads+=((i,t))
    }
    reader.readEndArray()
    
    reader.readName("tCtr")
    val tCtr = reader.readInt32()
    
    reader.readName("fCtr")
    val fCtr = reader.readInt32()
    
    reader.readName("procs")
	  reader.readStartArray()
    var procs:Queue[PiProcess] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      val name = reader.readString()
      processes.get(name) match {
        case None => throw new CodecConfigurationException("Unknown PiProcess: " + name)
        case Some(p) => procs+=p
      }
    }
    reader.readEndArray()
    
    reader.readName("map")
    val cMap = decoderContext.decodeWithChildContext(cmapCodec,reader)
    
	  reader.readEndDocument()
	  PiState(Map(inputs:_*),Map(outputs:_*),calls.toList,Map(threads:_*),tCtr,fCtr,PiProcess.mapOf(procs:_*),cMap)
  }
}