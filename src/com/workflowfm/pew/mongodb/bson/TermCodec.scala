package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue

class TermCodec(registry: CodecRegistry) extends Codec[Term] { 
  def this() = this(DEFAULT_CODEC_REGISTRY)
  
  val piobjCodec:Codec[PiObject] = registry.get(classOf[PiObject])
  
  override def encode(writer: BsonWriter, value: Term, encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeName("_t")
    value match {
      case Devour(c,v) => {
        writer.writeString("Devour")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("v")
        writer.writeString(v)
      }
      case In(c,v,cont) => {
    	  writer.writeString("In")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("v")
        writer.writeString(v)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this,writer,cont)
      }
      case ParIn(c,lv,rv,left,right) => {
     	  writer.writeString("ParIn")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cl")
        writer.writeString(lv)
        writer.writeName("cr")
        writer.writeString(rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,right)
      }
      case ParInI(c,lv,rv,cont) => {
     	  writer.writeString("ParInI")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cl")
        writer.writeString(lv)
        writer.writeName("cr")
        writer.writeString(rv)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this,writer,cont)
      }       
      case WithIn(c,lv,rv,left,right) => {
     	  writer.writeString("WithIn")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cl")
        writer.writeString(lv)
        writer.writeName("cr")
        writer.writeString(rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,right)
      } 
      case Out(c,obj) => {
     	  writer.writeString("Out")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("v")
        encoderContext.encodeWithChildContext(piobjCodec,writer,obj)
      }
      case ParOut(c,lv,rv,left,right) => {
     	  writer.writeString("ParOut")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cl")
        writer.writeString(lv)
        writer.writeName("cr")
        writer.writeString(rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,right)
      }
      case LeftOut(c,lc,cont) => {
     	  writer.writeString("LeftOut")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cl")
        writer.writeString(lc)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this,writer,cont)
      }
      case RightOut(c,rc,cont) => {
     	  writer.writeString("RightOut")
        writer.writeName("c")
        writer.writeString(c)
        writer.writeName("cr")
        writer.writeString(rc)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this,writer,cont)
      }
      case PiCut(z,cl,cr,left,right) => {
     	  writer.writeString("PiCut")
        writer.writeName("z")
        writer.writeString(z)
        writer.writeName("cl")
        writer.writeString(cl)
        writer.writeName("cr")
        writer.writeString(cr)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,right)
      }
      case PiCall(name, args) => { // TODO we might want to have an independent PiCall codec for PiState
     	  writer.writeString("PiCall")
        writer.writeName("name")
        writer.writeString(name)
        writer.writeName("args")
        writer.writeStartArray()
        for (arg <- args) encoderContext.encodeWithChildContext(piobjCodec,writer,arg)
        writer.writeEndArray()
      }
//encoderContext.encodeWithChildContext(this,writer,r)

   }
   writer.writeEndDocument() 
  }

  override def getEncoderClass: Class[Term] = classOf[Term]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): Term = {
    reader.readStartDocument()
    reader.readName() // "_t"
    val ret:Term = reader.readString() match {
      case "Devour" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("v")
        val v = reader.readString()
        Devour(c,v)
      }
      case "In" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("v")
        val v = reader.readString()
        reader.readName("t")
        val cont = decoderContext.decodeWithChildContext(this,reader)
        In(c,v,cont)
      }
      case "ParIn" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        ParIn(c,cl,cr,l,r)
      }
      case "ParInI" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this,reader)
        ParInI(c,cl,cr,t)
      }
      case "WithIn" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        WithIn(c,cl,cr,l,r)
      }
      case "Out" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("v")
        val v = decoderContext.decodeWithChildContext(piobjCodec,reader)
        Out(c,v)
      }
      case "ParOut" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        ParOut(c,cl,cr,l,r)
      }
      case "LeftOut" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this,reader)
        LeftOut(c,cl,t)
      }      
      case "RightOut" => {
        reader.readName("c")
        val c = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this,reader)
        RightOut(c,cr,t)
      }
      case "PiCut" => {
        reader.readName("z")
        val c = reader.readString()
        reader.readName("cl")
        val cl = reader.readString()
        reader.readName("cr")
        val cr = reader.readString()
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        PiCut(c,cl,cr,l,r)
      }
      case "PiCall" => {
        reader.readName("name")
        val n = reader.readString()
        reader.readName("args")
        reader.readStartArray()
        var args:Queue[Chan] = Queue()
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
          val a = decoderContext.decodeWithChildContext(piobjCodec,reader).asInstanceOf[Chan]
          args+=a
        }
        reader.readEndArray()
        PiCall(n,args.toSeq)
      }      
      //        val l = decoderContext.decodeWithChildContext(this,reader)
    }
    reader.readEndDocument()
    ret
  }
}

/**
 * CodecProvider for PiObjectCodec
 * 
 * Created using this as an example:
 * https://github.com/mongodb/mongo-scala-driver/blob/master/bson/src/main/scala/org/mongodb/scala/bson/codecs/DocumentCodecProvider.scala
 */
class TermCodecProvider(bsonTypeClassMap:BsonTypeClassMap) extends CodecProvider {
  val PROVIDEDCLASS:Class[Term] =  classOf[Term]  
  
  override def get[T](clazz:Class[T], registry:CodecRegistry):Codec[T] = clazz match {
      case PROVIDEDCLASS => new PiObjectCodec(registry).asInstanceOf[Codec[T]]
      case _ => null
    }
}