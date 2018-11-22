package com.workflowfm.pew.mongodb.bson.auto

import org.bson.codecs.Codec
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

import scala.collection.mutable

/** AutoCodecStore:
  * A mutable.Map which indexes Codecs by the type/class of object they serialise/deserialise.
  * Used to help implement `CodecRegistry` and `CodecProvider` interfaces which having to
  * manually implement the `get` functionality for each new `Codec`.
  */
trait AutoCodecStore {

  /** Hold all the Codecs which have been registered with this `AutoCodecStore`.
    */
  val registeredCodecs: mutable.Map[Class[_], Codec[_]] = mutable.Map()

  /** Register a new Codec with this `AutoCodecStore`
    *
     * @param codec The new codec to register with this `AutoCodecStore`.
    */
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

  /** Dumb helper inner-trait to automatically register instances of this trait with
    * the parent `AutoCodecStore`.
    */
  trait AutoCodec { this:Codec[_] =>
      registerCodec( this )
  }

}

/** Implements the `CodecRegistry` interface using an `AutoCodecStore`.
  */
trait AutoCodecRegistry extends AutoCodecStore with CodecRegistry {

  override def get[T](clazz: Class[T]): Codec[T]
    = registeredCodecs
      .get( clazz )
      .map( _.asInstanceOf[Codec[T]] )
      .orNull
}

/** Implements the `CodecProvider` interface using an `AutoCodecStore`.
  */
trait AutoCodecProvider extends AutoCodecStore with CodecProvider {

  override def get[T]( clazz: Class[T], reg: CodecRegistry ): Codec[T]
    = registeredCodecs
      .get( clazz )
      .map( _.asInstanceOf[Codec[T]] )
      .getOrElse( reg.get( clazz ) )

}

/** Implements both `CodecRegistry` and `CodecProvider` interfaces using an `AutoCodecStore`.
  */
trait AutoCodecRegistryExt
  extends AutoCodecProvider with CodecRegistry {

  /** Base or builtin `CodecRegistry` to fall back on if a Codec is requested for a class
    * which hasn't been registered.
    */
  val baseRegistry: CodecRegistry

  override def get[T](clazz: Class[T]): Codec[T]
    = get( clazz, baseRegistry )
}