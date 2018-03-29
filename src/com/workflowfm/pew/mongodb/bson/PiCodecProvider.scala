package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId

/**
 * Provider for Codecs for PiObject, Chan, ChanMap, PiResource, PiFuture
 * 
 * Created using this as an example:
 * https://github.com/mongodb/mongo-scala-driver/blob/master/bson/src/main/scala/org/mongodb/scala/bson/codecs/DocumentCodecProvider.scala
 */
class PiCodecProvider(processes:Map[String,PiProcess] = Map()) extends CodecProvider { //bsonTypeClassMap:BsonTypeClassMap
  val OBJCLASS:Class[PiObject] =  classOf[PiObject]
  val CHANCLASS:Class[Chan] =  classOf[Chan]
  val CHANMAPCLASS:Class[ChanMap] =  classOf[ChanMap]
  val RESOURCECLASS:Class[PiResource] = classOf[PiResource]
  val FUTURECLASS:Class[PiFuture] = classOf[PiFuture]
  val STATECLASS:Class[PiState] = classOf[PiState]
  val TERMCLASS:Class[Term] = classOf[Term]
  val INSTANCECLASS:Class[PiInstance[ObjectId]] = classOf[PiInstance[ObjectId]]
  
  override def get[T](clazz:Class[T], registry:CodecRegistry):Codec[T] = clazz match {
      case OBJCLASS => new PiObjectCodec(registry).asInstanceOf[Codec[T]]
      case CHANCLASS => new ChanCodec(registry).asInstanceOf[Codec[T]]
      case CHANMAPCLASS => new ChanMapCodec(registry).asInstanceOf[Codec[T]]
      case RESOURCECLASS => new PiResourceCodec(registry).asInstanceOf[Codec[T]]
      case FUTURECLASS => new PiFutureCodec(registry).asInstanceOf[Codec[T]]
      case STATECLASS => new PiStateCodec(registry,processes).asInstanceOf[Codec[T]]
      case TERMCLASS => new TermCodec(registry).asInstanceOf[Codec[T]]
      case INSTANCECLASS => new PiInstanceCodec(registry,processes).asInstanceOf[Codec[T]]
      case _ => null
    }
}