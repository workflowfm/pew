package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.pitypes._
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
class PiCodecProvider(processes:PiProcessStore = SimpleProcessStore()) extends CodecProvider { //bsonTypeClassMap:BsonTypeClassMap
  val OBJIDCLASS: Class[ObjectId] = classOf[ObjectId]
  val PIPROCCLASS: Class[PiProcess] = classOf[PiProcess]

  val OBJCLASS:Class[PiObject] =  classOf[PiObject]
  val CHANCLASS:Class[Chan] =  classOf[Chan]
  val CHANMAPCLASS:Class[ChanMap] =  classOf[ChanMap]
  val RESOURCECLASS:Class[PiResource] = classOf[PiResource]
  val FUTURECLASS:Class[PiFuture] = classOf[PiFuture]
  val STATECLASS:Class[PiState] = classOf[PiState]
  val TERMCLASS:Class[Term] = classOf[Term]
  val INSTANCECLASS:Class[PiInstance[ObjectId]] = classOf[PiInstance[ObjectId]]
  
  override def get[T](clazz:Class[T], registry:CodecRegistry):Codec[T]
    = (clazz match {
      case OBJIDCLASS     => new helper.ObjectIdCodec()
      case PIPROCCLASS    => new PiProcessCodec( processes )
      case OBJCLASS       => new PiObjectCodec(registry)
      case CHANCLASS      => new ChanCodec
      case CHANMAPCLASS   => new ChanMapCodec(registry)
      case RESOURCECLASS  => new PiResourceCodec(registry)
      case FUTURECLASS    => new PiFutureCodec(registry)
      case STATECLASS     => new PiStateCodec(registry,processes)
      case TERMCLASS      => new TermCodec(registry)
      case INSTANCECLASS  => new PiInstanceCodec(registry, processes)
      case _              => null
    }).asInstanceOf[Codec[T]]
}