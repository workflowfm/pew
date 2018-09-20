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

  case class ReduceRequest(
    pii:  PiInstance[ObjectId],
    args: Seq[(CallRef, PiObject)]
  ) {
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
  )

  case class PiiUpdate(
    pii:      PiInstance[ObjectId]
  )

  case class Assignment(
    pii:      PiInstance[ObjectId],
    callRef:  CallRef,
    done:     Boolean,
    process:  String, // AtomicProcess,
    args:     Seq[PiResource]
  )

  case class Result[T](
    pii:      PiInstance[ObjectId],
    callRef:  Option[CallRef],
    res:      T
  ) {

    def this( pi: PiInstance[ObjectId], res: T )
      = this( pi, None, res )

    def this( pi: PiInstance[ObjectId], callRef: CallRef, res: T )
      = this( pi, Some(callRef), res )
  }

  type ResultSuccess = Result[PiObject]
  type ResultFailure = Result[Throwable]
}