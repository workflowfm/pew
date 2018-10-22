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

  type CallResult = ( CallRef, PiObject )

  /** A PiiHistory message which helps collect outstanding SequenceRequests after a failure.
    */
  case class SequenceFailure(
    pii:  Either[ObjectId, PiInstance[ObjectId]],
    returns: Seq[CallResult],
    errors: Seq[PiExceptionEvent[ObjectId]]

  ) extends PiiHistory {

    def piiId: ObjectId
      = pii match {
        case Left( _piiId ) => _piiId
        case Right( _pii ) => _pii.id
      }
  }

  object SequenceFailure {
    def apply( id: ObjectId, ref: CallRef, piEx: PiException[ObjectId] ): SequenceFailure
      = SequenceFailure( Left(id), Seq((ref, null)), Seq( piEx.event ) )

//    def apply( pii: PiInstance[ObjectId], results: Seq[CallResult], failures: Seq[PiExceptionEvent[ObjectId]] ): SequenceFailure
//      = SequenceFailure( Right( pii ), results, failures )
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

  case class PiiLog( event: PiEvent[ObjectId] ) extends AnyMsg {
    def piiId: ObjectId = event.id
  }

  def piiId( msg: AnyMsg ): ObjectId
    = msg match {
      case m: ReduceRequest     => m.pii.id
      case m: SequenceRequest   => m.piiId
      case m: SequenceFailure   => m.piiId
      case m: PiiUpdate         => m.pii.id
      case m: Assignment        => m.pii.id
      case m: PiiLog            => m.piiId
    }
}