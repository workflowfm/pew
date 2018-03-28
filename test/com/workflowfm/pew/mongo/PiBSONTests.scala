package com.workflowfm.pew.mongo

import org.mongodb.scala.bson.codecs._
import org.mongodb.scala.bson.codecs.Macros
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries._
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.configuration.CodecRegistries
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.bson.io._
import java.nio.ByteBuffer

@RunWith(classOf[JUnitRunner])
class PiBSONTests extends FlatSpec with Matchers {
	
  //val p = new PiObjectCodecProvider
  
  val chanCodecProvider = Macros.createCodecProvider[Chan]()
  val pairCodecProvider = Macros.createCodecProvider[PiPair]()
  val optCodecProvider = Macros.createCodecProvider[PiOpt]()
  val leftCodecProvider = Macros.createCodecProvider[PiLeft]()
  val rightCodecProvider = Macros.createCodecProvider[PiRight]()
  val reg = fromRegistries( 
	      fromProviders( 
	        chanCodecProvider, 
	        pairCodecProvider, 
	        optCodecProvider,
	        leftCodecProvider,
	        rightCodecProvider),
	      fromCodecs(new PiItemCodec),
	      DEFAULT_CODEC_REGISTRY) 
  
  //val r = fromRegistries(fromProviders(p), DEFAULT_CODEC_REGISTRY)
  
  val obj = PiPair(PiItem("Oh!"),PiItem("Hi!"))
  
  val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
  reg.get(classOf[PiPair]).encode(writer, obj, EncoderContext.builder().build())
  System.err.println(">>>" + writer.getBsonOutput().asInstanceOf[BasicOutputBuffer].toByteArray())
  
  val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
  val reader: BsonBinaryReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))

  val dec = reg.get(classOf[PiPair]).decode(reader, DecoderContext.builder().build())
  System.err.println(">>>! " + dec)
  
  "ha" should "tell the truth" in {
		1 should be (1)
	}
}