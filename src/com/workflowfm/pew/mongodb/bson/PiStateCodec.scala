package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{CodecConfigurationException, CodecRegistry}

class PiStateCodec(registry:CodecRegistry,processes:PiProcessStore) extends Codec[PiState] { 
  val chanCodec:Codec[Chan] = registry.get(classOf[Chan])
  val termCodec:Codec[Term] = registry.get(classOf[Term])
  val futureCodec:Codec[PiFuture] = registry.get(classOf[PiFuture])
  val cmapCodec:Codec[ChanMap] = registry.get(classOf[ChanMap])

  import BsonUtil._

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

    val inputs: List[(Chan, Input)]
      = readArray( reader, "inputs" ) { () =>
        reader.readStartDocument()
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec,reader)
        reader.readName("i")
        val i = decoderContext.decodeWithChildContext(termCodec,reader).asInstanceOf[Input] // TODO handle exception
        reader.readEndDocument()
        (c,i)
      }

    val outputs: List[(Chan, Output)]
      = readArray( reader, "outputs" ) { () =>
        reader.readStartDocument()
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec,reader)
        reader.readName("o")
        val o = decoderContext.decodeWithChildContext(termCodec,reader).asInstanceOf[Output] // TODO handle exception
        reader.readEndDocument()
        (c,o)
      }

    val calls: List[PiFuture]
      = readArray( reader, "calls" ) { () =>
        decoderContext.decodeWithChildContext(futureCodec,reader)
      }

    val threads: List[(Int, PiFuture)]
      = readArray( reader, "threads" ) { () =>
        reader.readStartDocument()
        reader.readName("i")
        val i = reader.readInt32()
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(futureCodec,reader)
        reader.readEndDocument()
        (i,t)
      }
    
    reader.readName("tCtr")
    val tCtr = reader.readInt32()
    
    reader.readName("fCtr")
    val fCtr = reader.readInt32()

    val procs: List[PiProcess]
      = readArray( reader, "procs" ) { () =>
        val processName: String = reader.readString()
        processes.get( processName ) match {
          case None => throw new CodecConfigurationException("Unknown PiProcess: " + processName)
          case Some( process ) => process
        }
      }
    
    reader.readName("map")
    val cMap = decoderContext.decodeWithChildContext(cmapCodec,reader)
    
	  reader.readEndDocument()
	  PiState(
      Map(inputs:_*),
      Map(outputs:_*),
      calls,
      Map(threads:_*),
      tCtr,fCtr,
      PiProcessStore.mapOf(procs:_*),
      cMap
    )
  }
}