package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless._
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId

object PewCodecs {

  import KafkaExecutorSettings._
  import StatelessMessages._

  type PiiT     = PiInstance[ObjectId]
  type PiResT   = PiResource
  type ResMsgT  = PiiResult[AnyRes]

  val ANY_KEY:          Class[AnyKey]           = classOf[AnyKey]
  val ANY_MSG:          Class[AnyMsg]           = classOf[AnyMsg]
  val ANY_RES:          Class[AnyRes]           = classOf[AnyRes]

  val CALL_REF:         Class[CallRef]          = classOf[CallRef]
  val KEY_PII_ID:       Class[KeyPiiId]         = classOf[KeyPiiId]
  val KEY_PII_ID_CALL:  Class[KeyPiiIdCall]     = classOf[KeyPiiIdCall]
  val PII_UPDATE:       Class[PiiUpdate]        = classOf[PiiUpdate]
  val ASSIGNMENT:       Class[Assignment]       = classOf[Assignment]
  val REDUCE_REQUEST:   Class[ReduceRequest]    = classOf[ReduceRequest]
  val SEQUENCE_REQ:     Class[SequenceRequest]  = classOf[SequenceRequest]
  val RESULT_ANY_MSG:   Class[ResMsgT]          = classOf[ResMsgT]

}
