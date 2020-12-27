package com.workflowfm.pew.mongodb.bson.pitypes

import scala.collection.mutable.ArrayBuffer

import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistries, CodecRegistry }
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.BsonUtil

class TermCodec(registry: CodecRegistry) extends Codec[Term] {
  def this() = this(
    CodecRegistries.fromRegistries(
      CodecRegistries.fromCodecs(new PiObjectCodec),
      DEFAULT_CODEC_REGISTRY
    )
  )

  import BsonUtil._

  val piobjCodec: Codec[PiObject] = registry.get(classOf[PiObject])
  val chanCodec: Codec[Chan] = registry.get(classOf[Chan])

  override def encode(writer: BsonWriter, value: Term, encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeName("_t")
    value match {
      case Devour(c, v) => {
        writer.writeString("Devour")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("v")
        encoderContext.encodeWithChildContext(chanCodec, writer, v)
      }
      case In(c, v, cont) => {
        writer.writeString("In")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("v")
        encoderContext.encodeWithChildContext(chanCodec, writer, v)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this, writer, cont)
      }
      case ParIn(c, lv, rv, left, right) => {
        writer.writeString("ParIn")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, lv)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this, writer, left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this, writer, right)
      }
      case ParInI(c, lv, rv, cont) => {
        writer.writeString("ParInI")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, lv)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, rv)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this, writer, cont)
      }
      case WithIn(c, lv, rv, left, right) => {
        writer.writeString("WithIn")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, lv)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this, writer, left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this, writer, right)
      }
      case Out(c, obj) => {
        writer.writeString("Out")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("v")
        encoderContext.encodeWithChildContext(piobjCodec, writer, obj)
      }
      case ParOut(c, lv, rv, left, right) => {
        writer.writeString("ParOut")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, lv)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, rv)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this, writer, left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this, writer, right)
      }
      case LeftOut(c, lc, cont) => {
        writer.writeString("LeftOut")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, lc)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this, writer, cont)
      }
      case RightOut(c, rc, cont) => {
        writer.writeString("RightOut")
        writer.writeName("c")
        encoderContext.encodeWithChildContext(chanCodec, writer, c)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, rc)
        writer.writeName("t")
        encoderContext.encodeWithChildContext(this, writer, cont)
      }
      case PiCut(z, cl, cr, left, right) => {
        writer.writeString("PiCut")
        writer.writeName("z")
        encoderContext.encodeWithChildContext(chanCodec, writer, z)
        writer.writeName("cl")
        encoderContext.encodeWithChildContext(chanCodec, writer, cl)
        writer.writeName("cr")
        encoderContext.encodeWithChildContext(chanCodec, writer, cr)
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this, writer, left)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this, writer, right)
      }
      case PiCall(name, args) => { // TODO we might want to have an independent PiCall codec for PiState
        writer.writeString("PiCall")
        writer.writeName("name")
        writer.writeString(name)

        writeArray(writer, "args", args) {
          encoderContext.encodeWithChildContext(piobjCodec, writer, _)
        }
      }
//encoderContext.encodeWithChildContext(this,writer,r)

    }
    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[Term] = classOf[Term]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): Term = {
    reader.readStartDocument()
    reader.readName("_t")
    val ret: Term = reader.readString() match {
      case "Devour" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("v")
        val v = decoderContext.decodeWithChildContext(chanCodec, reader)
        Devour(c, v)
      }
      case "In" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("v")
        val v = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("t")
        val cont = decoderContext.decodeWithChildContext(this, reader)
        In(c, v, cont)
      }
      case "ParIn" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this, reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this, reader)
        ParIn(c, cl, cr, l, r)
      }
      case "ParInI" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this, reader)
        ParInI(c, cl, cr, t)
      }
      case "WithIn" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this, reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this, reader)
        WithIn(c, cl, cr, l, r)
      }
      case "Out" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("v")
        val v = decoderContext.decodeWithChildContext(piobjCodec, reader)
        Out(c, v)
      }
      case "ParOut" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this, reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this, reader)
        ParOut(c, cl, cr, l, r)
      }
      case "LeftOut" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this, reader)
        LeftOut(c, cl, t)
      }
      case "RightOut" => {
        reader.readName("c")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("t")
        val t = decoderContext.decodeWithChildContext(this, reader)
        RightOut(c, cr, t)
      }
      case "PiCut" => {
        reader.readName("z")
        val c = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cl")
        val cl = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("cr")
        val cr = decoderContext.decodeWithChildContext(chanCodec, reader)
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this, reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this, reader)
        PiCut(c, cl, cr, l, r)
      }
      case "PiCall" => {
        reader.readName("name")
        val n = reader.readString()

        val args: ArrayBuffer[Chan] = readArray(reader, "args") { () =>
          decoderContext
            .decodeWithChildContext(piobjCodec, reader)
            .asInstanceOf[Chan]
        }

        PiCall(n, args)
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
class TermCodecProvider(bsonTypeClassMap: BsonTypeClassMap) extends CodecProvider {
  val PROVIDEDCLASS: Class[Term] = classOf[Term]

  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = clazz match {
    case PROVIDEDCLASS => new TermCodec(registry).asInstanceOf[Codec[T]]
    case _ => null
  }
}
