package com.workflowfm.pew.stateless.instances.kafka.settings.bson

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson._
import com.workflowfm.pew.mongodb.bson.auto.{ AutoCodecRegistryExt, SuperclassCodec }
import com.workflowfm.pew.mongodb.bson.events._
import com.workflowfm.pew.mongodb.bson.helper._
import com.workflowfm.pew.mongodb.bson.pitypes._
import com.workflowfm.pew.stateless.StatelessMessages.{ AnyMsg, PiiHistory }
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.AnyKey
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.content.CallRefCodec
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.keys.{
  KeyPiiIdCallCodec,
  KeyPiiIdCodec
}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages._
import org.bson.codecs.Codec
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistry }
import org.bson.types.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

class KafkaCodecRegistry(
    processes: PiProcessStore,
    override val baseRegistry: CodecRegistry = DEFAULT_CODEC_REGISTRY
) extends CodecProvider
    with AutoCodecRegistryExt {

  /** AnyCodec for encoding arbitrary objects. Uses lazy value
    * to support being fully overridden by subclasses.
    * (Note: All types must have codecs present in this object
    * when it is actually time to encode/decode them at runtime)
    */
  protected lazy val anyc: Codec[Any] = registerCodec(new AnyCodec(this))

  registerCodec(new BoxedUnitCodec)
  registerCodec(new Tuple2Codec(anyc))
  registerCodec(EitherCodec(anyc))
  registerCodec(OptionCodec(anyc))

  /** A local type of PiObjectCodec to ensure the `anyCodec` used
    * matches the one overridden here.
    */
  class LocalPiObjectCodec extends PiObjectCodec(this) {
    override lazy val anyCodec: Codec[Any] = anyc
  }

  // Keep explicit references to these PEW codec instances,
  // We don't have a registry that includes them.
  private val idc = registerCodec(new ObjectIdCodec())
  private val procc = registerCodec(new PiProcessCodec(processes))
  private val obj = registerCodec(new LocalPiObjectCodec)
  private val term = registerCodec(new TermCodec(this))
  private val chan = registerCodec(new ChanCodec)
  private val chanMap = registerCodec(new ChanMapCodec(this))
  private val piRes = registerCodec(new PiResourceCodec(this))
  private val fut = registerCodec(new PiFutureCodec(this))
  private val piState = registerCodec(new PiStateCodec(this, processes))
  private val piInst = registerCodec(new PiInstanceCodec(this, processes))
  private val meta = registerCodec(new PiMetadataMapCodec(anyc))

  registerCodec(new PiEventCallCodec[ObjectId](idc, obj, procc, meta))
  registerCodec(new PiEventExceptionCodec[ObjectId](idc, meta))
  registerCodec(new PiEventProcessExceptionCodec[ObjectId](idc, meta))
  registerCodec(new PiEventResultCodec[ObjectId](piInst, anyc, meta))
  registerCodec(new PiEventReturnCodec[ObjectId](idc, anyc, meta))
  registerCodec(new PiEventStartCodec[ObjectId](piInst, meta))
  registerCodec(new PiFailureAtomicProcessIsCompositeCodec[ObjectId](piInst, meta))
  registerCodec(new PiFailureNoResultCodec[ObjectId](piInst, meta))
  registerCodec(new PiFailureNoSuchInstanceCodec[ObjectId](idc, meta))
  registerCodec(new PiFailureUnknownProcessCodec[ObjectId](piInst, meta))
  private val peExEvent = registerCodec(new SuperclassCodec[PiFailure[ObjectId]])
  private val peEvent = registerCodec(new SuperclassCodec[PiEvent[ObjectId]])

  // These use the PEW-REST Key codecs, need to be initialised after PEW
  private val callRef = registerCodec(new CallRefCodec)
  private val keyPiiId = registerCodec(new KeyPiiIdCodec)
  private val keyPiiIdCall = registerCodec(new KeyPiiIdCallCodec(callRef))

  // These use the PEW-REST Msg codecs, need to be initialised after PEW
  private val update = registerCodec(new PiiUpdateCodec(piInst))
  private val assgn = registerCodec(new AssignmentCodec(piInst, callRef, piRes))
  private val seqReq = registerCodec(new SequenceRequestCodec(callRef, obj))
  private val seqfail = registerCodec(new SequenceFailureCodec(piInst, callRef, obj, peExEvent))
  private val redReq = registerCodec(new ReduceRequestCodec(piInst, callRef, obj))
  private val res = registerCodec(new PiiLogCodec(peEvent))

  private val piiHistory = registerCodec(new SuperclassCodec[PiiHistory])

  // Initialised after both Keys & Msgs as it depends on them all.
  private val anykey = registerCodec(new SuperclassCodec[AnyKey])
  val anymsg: Codec[AnyMsg] = registerCodec(new SuperclassCodec[AnyMsg])
}
