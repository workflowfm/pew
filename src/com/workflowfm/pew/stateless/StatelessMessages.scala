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

  trait StatelessMessage
  trait PiiHistory extends StatelessMessage

  case class ReduceRequest(
    pii:  PiInstance[ObjectId],
    args: Seq[(CallRef, PiObject)]

  ) extends StatelessMessage {

    def this( pii: PiInstance[ObjectId] )
      = this( pii, Seq() )

    def add( newArg: (CallRef, PiObject) ): ReduceRequest
      = ReduceRequest( pii, args :+ newArg )

    def add( newArgs: Seq[(CallRef, PiObject)] ): ReduceRequest
      = ReduceRequest( pii, args ++ newArgs )
  }

  case class SequenceRequest(
    piiId: ObjectId,
    request: (CallRef, PiObject),

  ) extends PiiHistory

  case class PiiUpdate(
    pii:      PiInstance[ObjectId]

  ) extends PiiHistory

  case class Assignment(
    pii:      PiInstance[ObjectId],
    callRef:  CallRef,
    process:  String, // AtomicProcess,
    args:     Seq[PiResource]

  ) extends StatelessMessage

  case class PiiResult[T](
    pii:      PiInstance[ObjectId],
    callRef:  Option[CallRef],
    res:      T

  ) extends StatelessMessage {

    def this( pi: PiInstance[ObjectId], res: T )
      = this( pi, None, res )

    def this( pi: PiInstance[ObjectId], callRef: CallRef, res: T )
      = this( pi, Some(callRef), res )
  }

  type ResultSuccess = PiiResult[PiObject]
  type ResultFailure = PiiResult[Throwable]
}