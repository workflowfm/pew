package com.workflowfm.pew.stateless.instances.kafka.settings.bson

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson._
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs._
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.content.{AnyResCodec, CallRefCodec, ThrowableCodec}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.keys.{AnyKeyCodec, KeyPiiIdCallCodec, KeyPiiIdCodec}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages._
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

class KafkaCodecRegistry(
    processes: PiProcessStore,
    baseRegistry: CodecRegistry = DEFAULT_CODEC_REGISTRY

  ) extends PiCodecProvider( processes )
  with CodecRegistry {

  import PewCodecs._

  // Keep explicit references to these PEW codec instances,
  // We don't have a registry that includes them.
  private val obj = new PiObjectCodec(this)
  private val term = new TermCodec(this)
  private val chan = new ChanCodec(this)
  private val chanMap = new ChanMapCodec(this)
  private val piRes = new PiResourceCodec(this)
  private val fut =  new PiFutureCodec(this)
  private val piState = new PiStateCodec(this, processes)
  private val piInst = new PiInstanceCodec(this, processes)

  // Needs to be initialised before any 'ResultCodec' which depend on it.
  private val throwable = new ThrowableCodec
  val anyres: Codec[Any] = new AnyResCodec( obj, throwable )

  // These use the PEW-REST Key codecs, need to be initialised after PEW
  private val callRef = new CallRefCodec
  private val keyPiiId = new KeyPiiIdCodec
  private val keyPiiIdCall = new KeyPiiIdCallCodec( callRef )

  // These use the PEW-REST Mgs codecs, need to be initialised after PEW
  private val update = new PiiUpdateCodec( piInst )
  private val assgn = new AssignmentCodec( piInst, callRef, piRes )
  private val seqReq = new SequenceRequestCodec( callRef, obj )
  private val seqfail = new SequenceFailureCodec( piInst, callRef, obj, throwable )
  private val redReq = new ReduceRequestCodec( piInst, callRef, obj )
  private val res = new PiiLogCodec( this )
  private val piiHistory = new PiiHistoryCodec( seqReq, update, seqfail)

  // Initialised after both Keys & Msgs as it depends on them all.
  private val anykey = new AnyKeyCodec( keyPiiId, keyPiiIdCall )
  val anymsg: Codec[AnyMsg] = new AnyMsgCodec( this )

  /** Implement the get[T] method from Codec*REGISTRY*,
    * - Needed by the PEW codecs which interface with this as a CodecRegistry.
    *
    * @return Necessary codec from baseRegistry for basic types or our own codec
    *         instances from PEW or PEW-REST types.
    */
  override def get[T](clazz: Class[T]): Codec[T] = {
    get( clazz, baseRegistry )
  }

  override def get[T]( clazz: Class[T], reg: CodecRegistry )
    : Codec[T] = ( clazz match {

    case PIEVENT              =>
    case PISTART              =>
    case PIRESULT             =>
    case PICALL               =>
    case PIRETURN             =>
    case PINORES              =>
    case PIUNKNOWN            =>
    case PIFAPIS              =>
    case PIFNSI               =>
    case PIEXCEPT             =>
    case PIPROCEXCEPT         =>

    case OBJCLASS             => obj
    case CHANCLASS            => chan
    case CHANMAPCLASS         => chanMap
    case RESOURCECLASS        => piRes
    case FUTURECLASS          => fut
    case STATECLASS           => piState
    case TERMCLASS            => term
    case INSTANCECLASS        => piInst

    case THROWABLE            => throwable
    case CALL_REF             => callRef
    case KEY_PII_ID           => keyPiiId
    case KEY_PII_ID_CALL      => keyPiiIdCall

    case PII_UPDATE           => update
    case ASSIGNMENT           => assgn
    case SEQUENCE_REQ         => seqReq
    case SEQFAIL_REQ          => seqfail
    case REDUCE_REQUEST       => redReq
    case PIILOG               => res

    case PII_HISTORY          => piiHistory

    // TODO, How are these being differentiated, they're the same type ?!?!?!
    case ANY_KEY              => anykey
    case ANY_MSG              => anymsg
    case ANY_RES              => anyres

    case _                    => reg.get( clazz )

  }).asInstanceOf[ Codec[T] ]
}
