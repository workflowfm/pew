package com.workflowfm.pew.stateless.instances.kafka.settings.bson

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson._
import com.workflowfm.pew.mongodb.bson.auto.{AutoCodecRegistryExt, SuperclassCodec}
import com.workflowfm.pew.mongodb.bson.events._
import com.workflowfm.pew.mongodb.bson.helper.{ObjectIdCodec, Tuple2Codec}
import com.workflowfm.pew.mongodb.bson.pitypes._
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, PiiHistory}
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.AnyKey
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.content.CallRefCodec
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.keys.{KeyPiiIdCallCodec, KeyPiiIdCodec}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages._
import org.bson.codecs.Codec
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

class KafkaCodecRegistry(
    processes: PiProcessStore,
    override val baseRegistry: CodecRegistry = DEFAULT_CODEC_REGISTRY

  ) extends CodecProvider with AutoCodecRegistryExt {

  // AnyCodec for encoding arbitrary objects
  // (Note: All types must have codecs present in this object
  // when it is actually time to encode/decode them at runtime)
  private val anyc: Codec[Any] = new AnyCodec( this )

  new Tuple2Codec( anyc ) with AutoCodec

  new PiObjectCodec() with AutoCodec

  // Keep explicit references to these PEW codec instances,
  // We don't have a registry that includes them.
  private val idc = new ObjectIdCodec() with AutoCodec
  private val procc = new PiProcessCodec( processes ) with AutoCodec
  private val obj = new PiObjectCodec( this ) with AutoCodec
  private val term = new TermCodec(this) with AutoCodec
  private val chan = new ChanCodec with AutoCodec
  private val chanMap = new ChanMapCodec(this) with AutoCodec
  private val piRes = new PiResourceCodec(this) with AutoCodec
  private val fut =  new PiFutureCodec(this) with AutoCodec
  private val piState = new PiStateCodec(this, processes) with AutoCodec
  private val piInst = new PiInstanceCodec(this, processes) with AutoCodec
  private val times = new PiTimesCodec() with AutoCodec

  new PiEventCallCodec[ObjectId]( idc, obj, procc, times ) with AutoCodec
  new PiEventExceptionCodec[ObjectId]( idc, times ) with AutoCodec
  new PiEventProcessExceptionCodec[ObjectId]( idc, times ) with AutoCodec
  new PiEventResultCodec[ObjectId]( piInst, anyc, times ) with AutoCodec
  new PiEventReturnCodec[ObjectId]( idc, anyc, times ) with AutoCodec
  new PiEventStartCodec[ObjectId]( piInst, times ) with AutoCodec
  new PiFailureAtomicProcessIsCompositeCodec[ObjectId]( piInst, times ) with AutoCodec
  new PiFailureNoResultCodec[ObjectId]( piInst, times ) with AutoCodec
  new PiFailureNoSuchInstanceCodec[ObjectId]( idc, times ) with AutoCodec
  new PiFailureUnknownProcessCodec[ObjectId]( piInst, times ) with AutoCodec
  private val peExEvent = new SuperclassCodec[PiExceptionEvent[ObjectId]] with AutoCodec
  private val peEvent = new SuperclassCodec[PiEvent[ObjectId]] with AutoCodec

  // These use the PEW-REST Key codecs, need to be initialised after PEW
  private val callRef = new CallRefCodec with AutoCodec
  private val keyPiiId = new KeyPiiIdCodec with AutoCodec
  private val keyPiiIdCall = new KeyPiiIdCallCodec( callRef ) with AutoCodec

  // These use the PEW-REST Msg codecs, need to be initialised after PEW
  private val update = new PiiUpdateCodec( piInst ) with AutoCodec
  private val assgn = new AssignmentCodec( piInst, callRef, piRes ) with AutoCodec
  private val seqReq = new SequenceRequestCodec( callRef, obj ) with AutoCodec
  private val seqfail = new SequenceFailureCodec( piInst, callRef, obj, peExEvent ) with AutoCodec
  private val redReq = new ReduceRequestCodec( piInst, callRef, obj ) with AutoCodec
  private val res = new PiiLogCodec( peEvent ) with AutoCodec

  private val piiHistory = new SuperclassCodec[PiiHistory] with AutoCodec


  // Initialised after both Keys & Msgs as it depends on them all.
  private val anykey = new SuperclassCodec[AnyKey] with AutoCodec
  val anymsg: Codec[AnyMsg] = new SuperclassCodec[AnyMsg] with AutoCodec
}
