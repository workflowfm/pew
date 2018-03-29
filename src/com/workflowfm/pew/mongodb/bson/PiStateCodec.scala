package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue

/*
 * case class PiFuture(fun:String, outChan:Chan, args:Seq[(PiObject,Chan)])
 * case class PiState(inputs:Map[Chan,Input], 
 *                    outputs:Map[Chan,Output], 
 *                    calls:List[PiFuture], 
 *                    threads:Map[Int,PiFuture], 
 *                    threadCtr:Int, 
 *                    freshCtr:Int, 
 *                    processes:Map[String,PiProcess], 
 *                    resources:ChanMap)
 */

/*
 * 
 * ERROR ERROR ERROR
 * This won't work because of type erasure!! The Provider will match any pair type with the first one it encounters.
 * e.g. a (PiObject,Chan) and a (Chan,Chan) are indistinguisable!
 * Create custom classes/bson docs instead.
 *  
 */
class PairCodec[A,B](leftCodec:Codec[A], rightCodec:Codec[B]) extends Codec[(A,B)] { 
  override def encode(writer: BsonWriter, value: (A,B), encoderContext: EncoderContext): Unit = value match {
    case (l,r) => {
      writer.writeStartDocument()
      writer.writeName("_1")
      encoderContext.encodeWithChildContext(leftCodec, writer, l)
      writer.writeName("_2")
      encoderContext.encodeWithChildContext(rightCodec, writer, r)
  	  writer.writeEndDocument() 
    }
  }
 
  override def getEncoderClass: Class[(A,B)] = classOf[(A,B)]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): (A,B) = {
    reader.readStartDocument()
    reader.readName("_1")
    val l = decoderContext.decodeWithChildContext(leftCodec, reader)
    reader.readName("_2")
    val r = decoderContext.decodeWithChildContext(rightCodec, reader)
    reader.readEndDocument()
    (l,r)
  }
}

class PairCodecProvider[A,B](leftClass:Class[A], rightClass:Class[B]) extends CodecProvider {
  val PROVIDEDCLASS:Class[(A,B)] = classOf[(A,B)]  
  
  // TODO careful here because type erasure matches any pair type!
  override def get[T](clazz:Class[T], registry:CodecRegistry):Codec[T] = clazz match {
      case PROVIDEDCLASS => new PairCodec(registry.get(leftClass),registry.get(rightClass)).asInstanceOf[Codec[T]]
      case _ => null
    }
}

class PiFutureCodec {
  
}

class PiStateCodec {
  
}