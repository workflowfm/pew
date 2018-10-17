package com.workflowfm.pew.mongodb.bson.auto

import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry

import scala.collection.mutable

trait AutoCodecRegistry extends CodecRegistry {

  val registeredCodecs: mutable.Map[ Class[_], Codec[_] ] = mutable.Map()

  protected def registerCodec( codec: TypedCodec[_] )
    : Unit = this.synchronized {

    if ( registeredCodecs contains codec.clazz )
      println(s"AutoCodecRegistry: Stomping existing registry for class '${codec.clazz.getName}'.")
    else
      println(s"AutoCodecRegistry: Registered codec for class '${codec.clazz.getName}'.")

    registeredCodecs.update( codec.clazz, codec )

    codec match {

      // Ensure each 'SuperclassCodec' is kept up-to-date with all it's subtype codecs.
      case classCodec: ClassCodec[_] =>
        registeredCodecs.values.collect({
          case superclassCodec: SuperclassCodec[_] =>
            superclassCodec.updateWith( classCodec )
        })

      // Ensure we catch all pre-existing ClassCodecs upon being created.
      case superclassCodec: SuperclassCodec[_] =>
        registeredCodecs.values.collect({
          case classCodec: ClassCodec[_] =>
            superclassCodec.updateWith( classCodec )
        })
    }
  }

  trait AutoCodec {
    val typed: TypedCodec[_] = this.asInstanceOf[TypedCodec[_]]
    if (typed != null) registerCodec( typed )
  }

  override def get[T](clazz: Class[T]): Codec[T]
    = registeredCodecs
      .get( clazz )
      .map( _.asInstanceOf[Codec[T]] )
      .orNull
}