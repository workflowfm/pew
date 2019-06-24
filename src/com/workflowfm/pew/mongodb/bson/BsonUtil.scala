package com.workflowfm.pew.mongodb.bson

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.bson.{BsonReader, BsonType, BsonWriter}
import org.mongodb.scala.bson
import org.mongodb.scala.bson.BsonBinary

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

object BsonUtil {

  def writeArray[T](writer: BsonWriter, name: String, col: Seq[T])(fn: T => Unit): Unit = {
    writer.writeStartArray(name)
    col.foreach(fn)
    writer.writeEndArray()
  }

  def readArray[Elem, That](reader: BsonReader, name: String)(
      fn: () => Elem
  )(implicit builder: CanBuildFrom[_, Elem, That]): That = {

    reader.readName(name)
    reader.readStartArray()
    var args: mutable.Builder[Elem, That] = builder.apply()

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) args += fn()

    reader.readEndArray()
    args.result()
  }

  def writeObjectSeq[Elem](writer: BsonWriter, name: String, col: Seq[Elem]): Unit = {

    val data: Array[Byte] = {
      val baos = new ByteArrayOutputStream()

      val oos = new ObjectOutputStream(baos)
      oos.writeInt(col.size)
      col foreach oos.writeObject
      oos.close()

      baos.toByteArray
    }

    writer.writeBinaryData(name, bson.BsonBinary(data))
  }

  def readObjectSeq[Elem, That](reader: BsonReader, name: String)(
      implicit builder: CanBuildFrom[_, Elem, That]
  ): That = {

    val binary: BsonBinary = reader.readBinaryData(name)
    val ois                = new ObjectInputStream(new ByteArrayInputStream(binary.getData))

    val size: Int = ois.readInt()

    val that: mutable.Builder[Elem, That] = builder.apply()
    for (_ <- 1 to size) {
      that += ois.readObject().asInstanceOf[Elem]
    }

    ois.close()
    that.result()
  }
}
