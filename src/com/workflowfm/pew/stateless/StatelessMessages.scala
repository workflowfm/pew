package com.workflowfm.pew.stateless

import com.workflowfm.pew._
import org.bson.types.ObjectId

/** All state is saved in "message logs" or "channels" between the actors involved.
  * When a new message is sent with the same indexes, the message is considered to update the value
  * of any previous messages. This behaviour allows us to create "mutable" databases from the persistent
  * message logs, which in turn offer reliability and scalability.
  *
  */
object StatelessMessages {

  trait AnyMsg
  trait PiiHistory extends AnyMsg

  case class ReduceRequest(
    pii:  PiInstance[ObjectId],
    args: Seq[(CallRef, PiObject)]

  ) extends AnyMsg

  case class SequenceRequest(
    piiId: ObjectId,
    request: (CallRef, PiObject)

  ) extends PiiHistory

  case class SequenceFailure(
    pii:  Either[ObjectId, PiInstance[ObjectId]],
    results: Seq[(CallRef, PiObject)],
    failures: Seq[(CallRef, Throwable)]

  ) extends PiiHistory {

    def piiId: ObjectId
      = pii match {
        case Left( _piiId ) => _piiId
        case Right( _pii ) => _pii.id
      }
  }

  case class PiiUpdate(
    pii:      PiInstance[ObjectId]

  ) extends PiiHistory

  case class Assignment(
    pii:      PiInstance[ObjectId],
    callRef:  CallRef,
    process:  String, // AtomicProcess,
    args:     Seq[PiResource]

  ) extends AnyMsg

  case class PiiResult[T](
    pii:      PiInstance[ObjectId],
    res:      T

  ) extends AnyMsg

  type ResultSuccess = PiiResult[PiObject]
  type ResultFailure = PiiResult[Throwable]

  def piiId( msg: AnyMsg ): ObjectId
    = msg match {
      case m: ReduceRequest     => m.pii.id
      case m: SequenceRequest   => m.piiId
      case m: PiiUpdate         => m.pii.id
      case m: Assignment        => m.pii.id
      case m: PiiResult[_]      => m.pii.id
    }
}