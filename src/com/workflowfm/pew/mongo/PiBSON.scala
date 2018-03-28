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


class PiItemCodec extends Codec[PiItem[_]] {  
  override def encode(writer: BsonWriter, value: PiItem[_], encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeName("piitem")
    value match {
    /*case PiItem(v: String) => writer.writeString(wrap(v))
    case PiItem(v: Int) => writer.writeString(wrap(v.toString()))
    case PiItem(v: Long) => writer.writeString(wrap(v.toString()))
    case PiItem(v: Boolean) => writer.writeString(wrap(v.toString()))
    case PiItem(v: ObjectId) => writer.writeString(wrap(v.toString()))*/
    case PiItem(v: String) => writer.writeString(v)
    case PiItem(v: Int) => writer.writeInt32(v)
    case PiItem(v: Long) => writer.writeInt64(v)
    case PiItem(v: Boolean) => writer.writeBoolean(v)
    case PiItem(v: ObjectId) => writer.writeObjectId(v) 
    
   }
   writer.writeEndDocument() }

  override def getEncoderClass: Class[PiItem[_]] = classOf[PiItem[_]]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiItem[_] = {
    reader.readStartDocument()
    reader.readName()
    val ret = reader.getCurrentBsonType match {
      case BsonType.BOOLEAN =>PiItem(reader.readBoolean())
      case BsonType.STRING => PiItem(reader.readString())
      case BsonType.INT64 => PiItem(reader.readInt64())
      case BsonType.INT32 => PiItem(reader.readInt32())
      case BsonType.OBJECT_ID => PiItem(reader.readObjectId())
    }
    reader.readEndDocument()
    ret
  }
}
//org.bson.BsonInvalidOperationException: writeString can only be called when State is VALUE, not when State is NAME
	
//class PiObjectCodec(registry:CodecRegistry) extends Codec[PiObject] {
//    override def encode(writer:BsonWriter, obj:PiObject, encoderContext:EncoderContext) = {
//        writeMap(writer, document, encoderContext);
//    }
//
//    override def decode(reader:BsonReader, decoderContext:DecoderContext) = {
//        Document document = new Document();
//
//        reader.readStartDocument();
//        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
//            String fieldName = reader.readName();
//            document.put(fieldName, readValue(reader, decoderContext));
//        }
//
//        reader.readEndDocument();
//
//        return document;
//    }
//
//    override def getEncoderClass() = classOf[PiObject]
//}

class PiObjectCodecProvider extends CodecProvider {
  val chanCodecProvider = Macros.createCodecProvider[Chan]()
  val pairCodecProvider = Macros.createCodecProvider[PiPair]()
  val optCodecProvider = Macros.createCodecProvider[PiOpt]()
  val leftCodecProvider = Macros.createCodecProvider[PiLeft]()
  val rightCodecProvider = Macros.createCodecProvider[PiRight]()
 
  @Override                                                                                          
  def get[T](clazz:Class[T], registry:CodecRegistry): Codec[T] =                     
  if (clazz == classOf[PiObject]) {                      
	  val reg = fromRegistries( 
	      fromProviders( 
	        chanCodecProvider, 
	        pairCodecProvider, 
	        optCodecProvider,
	        leftCodecProvider,
	        rightCodecProvider),
	      fromCodecs(new PiItemCodec),
	      registry)    
	  reg.get(clazz)           
  }                                                                                              

  // CodecProvider returns null if it's not a provider for the requresed Class 
  else null                                          
}
