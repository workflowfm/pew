package com.workflowfm.pew.mongodb.bson.auto

import org.bson.codecs.Codec
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

import scala.collection.mutable

trait AutoCodecStore {

  val registeredCodecs: mutable.Map[Class[_], Codec[_]] = mutable.Map()

  protected def registerCodec(codec: Codec[_])
    : Unit = this.synchronized {

    val clazz: Class[_] = codec.getEncoderClass

    if (registeredCodecs contains clazz)
      println(s"AutoCodecRegistry: Stomping existing registry for class '${clazz.getName}'.")
    else
      println(s"AutoCodecRegistry: Registered codec for class '${clazz.getName}'.")

    registeredCodecs.update(clazz, codec)

    codec match {

      // Ensure each 'SuperclassCodec' is kept up-to-date with all it's subtype codecs.
      case classCodec: ClassCodec[_] =>
        registeredCodecs.values.collect({
          case superclassCodec: SuperclassCodec[_] =>
            superclassCodec.updateWith(classCodec)
        })

      // Ensure we catch all pre-existing ClassCodecs upon being created.
      case superclassCodec: SuperclassCodec[_] =>
        registeredCodecs.values.collect({
          case classCodec: ClassCodec[_] =>
            superclassCodec.updateWith(classCodec)
        })

        // Register all new child codecs as well.
        superclassCodec.knownChildren
          .filterNot(codec => registeredCodecs.contains(codec.clazz))
          .foreach(registerCodec)

      case _: Codec[_] => // No actions needed.
    }
  }

  trait AutoCodec {
    this match {
      case codec: Codec[_] =>
        registerCodec( codec )
    }
  }

}

trait AutoCodecRegistry extends AutoCodecStore with CodecRegistry {

  override def get[T](clazz: Class[T]): Codec[T]
    = registeredCodecs
      .get( clazz )
      .map( _.asInstanceOf[Codec[T]] )
      .orNull
}

trait AutoCodecProvider extends AutoCodecStore with CodecProvider {

  override def get[T]( clazz: Class[T], reg: CodecRegistry ): Codec[T]
    = registeredCodecs
      .get( clazz )
      .map( _.asInstanceOf[Codec[T]] )
      .getOrElse( reg.get( clazz ) )

}

trait AutoCodecRegistryExt
  extends AutoCodecProvider with CodecRegistry {

  val baseRegistry: CodecRegistry

  override def get[T](clazz: Class[T]): Codec[T]
    = get( clazz, baseRegistry )
}