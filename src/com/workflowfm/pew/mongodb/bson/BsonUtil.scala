package com.workflowfm.pew.mongodb.bson

import org.bson.{BsonReader, BsonType, BsonWriter}

import scala.collection.mutable

object BsonUtil {

  def writeArray[T]( writer: BsonWriter, name: String, col: Seq[T] )( fn: T => Unit ): Unit = {
    writer.writeStartArray( name )
    col.foreach( fn )
    writer.writeEndArray()
  }

  def readArray[T]( reader: BsonReader, name: String )( fn: () => T ): Seq[T] = {
    reader.readName( name )
    reader.readStartArray()
    var args: mutable.Queue[T] = mutable.Queue()

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
      args += fn()

    reader.readEndArray()
    args
  }

}
