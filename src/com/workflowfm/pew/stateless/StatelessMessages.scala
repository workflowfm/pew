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

  /** A PiiHistory message which helps collect outstanding SequenceRequests after a failure.
    *
    * @param pii
    * @param results Any collected call results that have yet to be sequenced.
    * @param failures The failed AtomicProcess calls and their errors.
    */
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

  object SequenceFailure {
    def apply( id: ObjectId, ref: CallRef, err: Throwable ): SequenceFailure
      = SequenceFailure( Left(id), Seq(), Seq((ref, err)) )

    def apply( pii: PiInstance[ObjectId], results: Seq[(CallRef, PiObject)], failures: Seq[(CallRef, Throwable)] ): SequenceFailure
      = SequenceFailure( Right( pii ), results, failures )
  }

  /** Wrapper for thrown exceptions which need to be serialised and deserialised.
    *
    * @param message Debug message.
    */
  case class RemoteExecutorException( message: String ) extends Exception( message )

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
      case m: SequenceFailure   => m.piiId
      case m: PiiUpdate         => m.pii.id
      case m: Assignment        => m.pii.id
      case m: PiiResult[_]      => m.pii.id
    }
}