package com.workflowfm.pew.mongodb.bson

import org.bson.{BsonReader, BsonType, BsonWriter}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

object BsonUtil {

  def writeArray[T]( writer: BsonWriter, name: String, col: Seq[T] )( fn: T => Unit ): Unit = {
    writer.writeStartArray( name )
    col.foreach( fn )
    writer.writeEndArray()
  }

  def readArray[Elem, That]
    ( reader: BsonReader, name: String )
    ( fn: () => Elem )
    ( implicit builder: CanBuildFrom[_, Elem, That] )
    : That = {

    reader.readName( name )
    reader.readStartArray()
    var args: mutable.Builder[Elem, That] = builder.apply()

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
      args += fn()

    reader.readEndArray()
    args.result()
  }

}
