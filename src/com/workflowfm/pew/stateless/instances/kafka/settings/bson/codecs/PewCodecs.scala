package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless._
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.{BsonReader, BsonType, BsonWriter}
import org.bson.types.ObjectId

import scala.collection.mutable

object PewCodecs {

  import KafkaExecutorSettings._
  import StatelessMessages._

  type PiiT     = PiInstance[ObjectId]
  type PiResT   = PiResource
  type ResMsgT  = PiiResult[AnyRes]

  val ANY_KEY:          Class[AnyKey]           = classOf[AnyKey]
  val ANY_MSG:          Class[AnyMsg]           = classOf[AnyMsg]
  val ANY_RES:          Class[AnyRes]           = classOf[AnyRes]

  val THROWABLE:        Class[Throwable]        = classOf[Throwable]
  val CALL_REF:         Class[CallRef]          = classOf[CallRef]
  val KEY_PII_ID:       Class[KeyPiiId]         = classOf[KeyPiiId]
  val KEY_PII_ID_CALL:  Class[KeyPiiIdCall]     = classOf[KeyPiiIdCall]
  val PII_UPDATE:       Class[PiiUpdate]        = classOf[PiiUpdate]
  val ASSIGNMENT:       Class[Assignment]       = classOf[Assignment]
  val PII_HISTORY:      Class[PiiHistory]       = classOf[PiiHistory]
  val REDUCE_REQUEST:   Class[ReduceRequest]    = classOf[ReduceRequest]
  val SEQUENCE_REQ:     Class[SequenceRequest]  = classOf[SequenceRequest]
  val SEQFAIL_REQ:      Class[SequenceFailure]  = classOf[SequenceFailure]
  val RESULT_ANY_MSG:   Class[ResMsgT]          = classOf[ResMsgT]

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
