package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs
import com.workflowfm.pew.util.ClassMap
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId

class PiiLogCodec( registry: CodecRegistry )
  extends Codec[PewCodecs.ResMsgT] {

  import PewCodecs._
  import StatelessMessages._

  type PiEventChildCodec = Codec[_ <: PiEvent[ObjectId]]

  val children: ClassMap[PiEventChildCodec]
    = ClassMap[PiEventChildCodec](
      registry.get( classOf[PiEventStart[ObjectId]] ),
      registry.get( classOf[PiEventResult[ObjectId]] ),
      registry.get( classOf[PiEventCall[ObjectId]] ),
      registry.get( classOf[PiEventReturn[ObjectId]] ),
      registry.get( classOf[PiFailureNoResult[ObjectId]] ),
      registry.get( classOf[PiFailureUnknownProcess[ObjectId]] ),
      registry.get( classOf[PiFailureAtomicProcessIsComposite[ObjectId]] ),
      registry.get( classOf[PiFailureNoSuchInstance[ObjectId]] ),
      registry.get( classOf[PiEventException[ObjectId]] ),
      registry.get( classOf[PiEventProcessException[ObjectId]] )
    )

  val typeN: String = "__type__"
  val childN: String = "child"

  override def decode(reader: BsonReader, ctx: DecoderContext): PiiLog = {
    reader.readStartDocument()

    val typ: String = reader.readString( typeN )

    reader.readName( childN )
    val childCodec: PiEventChildCodec = children.byName( typ ).head
    val content: PiEvent[ObjectId] = ctx.decodeWithChildContext( childCodec, reader )

    reader.readEndDocument()
    PiiLog( content )
  }

  override def encode(writer: BsonWriter, value: PiiLog, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    val event: PiEvent[ObjectId] = value.event
    writer.writeString( typeN, value.getClass.getSimpleName )

    writer.writeName( childN )
    val childCodec: Codec[event.type] = children[Codec[event.type]].head
    ctx.encodeWithChildContext( childCodec.asInstanceOf[Codec[PiEvent[ObjectId]]], writer, event )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[ResMsgT] = PIILOG

}