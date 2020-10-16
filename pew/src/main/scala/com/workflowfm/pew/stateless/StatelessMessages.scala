package com.workflowfm.pew.stateless

import org.bson.types.ObjectId

import com.workflowfm.pew._

/** All state is saved in "message logs" or "channels" between the actors involved.
  * When a new message is sent with the same indexes, the message is considered to update the value
  * of any previous messages. This behaviour allows us to create "mutable" databases from the persistent
  * message logs, which in turn offer reliability and scalability.
  */
object StatelessMessages {

  /** Superclass of all messages sent for a StatelessExecutor.
    */
  sealed trait AnyMsg {
    def piiId: ObjectId
  }

  /** `AnyMsg` which has a full PiInstance rather than just a pii id.
    */
  trait HasPii extends AnyMsg {
    val pii: PiInstance[ObjectId]
    override def piiId: ObjectId = pii.id
  }

  /** Superclass of all messages sent to the PiiHistory topic.
    */
  sealed trait PiiHistory extends AnyMsg

  /** A tuple identifying a result for a specific AtomicProcess execution.
    */
  type CallResult = (CallRef, PiObject)

  /** Contains the necessary information for reducing a PiInstance.
    *
    * @param pii The latest PiInstance to reduce and post-results into.
    * @param args A collection of result objects for open threads that need to be posted.
    */
  case class ReduceRequest(
      pii: PiInstance[ObjectId],
      args: Seq[CallResult]
  ) extends AnyMsg
      with HasPii {

    override def toString: String = s"ReduceRequest($piiId, $args)"
  }

  /** Emitted by a AtomicProcess executors to sequence their results into a common timeline.
    * Consumed by Sequencers to produce ReduceRequests.
    *
    * @param piiId ID of the PiInstance being executed, PiInstance state needs to be fetched
    *              from the PiiHistory topic.
    * @param request The result of the AtomicProcess call, containing the unique id and a
    *                PiObject representing the result.
    */
  case class SequenceRequest(
      piiId: ObjectId,
      request: CallResult
  ) extends PiiHistory

  /** A PiiHistory message which helps collect outstanding SequenceRequests after a failure.
    *
    * @param pii Identifying information for the PiInstance. Just an ObjectId if the state
    *            hasn't been seen, or the latest PiInstance.
    *
    * @param returns All the known results for the returned calls, the PiObjects may be null
    *                in the event of a failure.
    *
    * @param errors All the known exceptions encountered during the execution of this PiInstance.
    */
  case class SequenceFailure(
      pii: Either[ObjectId, PiInstance[ObjectId]],
      returns: Seq[CallResult],
      errors: Seq[PiFailure[ObjectId]]
  ) extends PiiHistory {

    override def piiId: ObjectId = pii match {
      case Left(_piiId) => _piiId
      case Right(_pii) => _pii.id
    }

    override def toString: String = s"SequenceFailure($piiId, $returns, $errors)"
  }

  object SequenceFailure {

    /** Constructs a SequenceFailure from a single failed AtomicProcess call.
      *
      * @param id The ID of the PiInstance being run.
      * @param ref The AtomicProcess call reference id which failed.
      * @param piEx The exception which was encountered.
      * @return A Sequence failure object containing a single CallRef and ExceptionEvent.
      */
    def apply(id: ObjectId, ref: CallRef, piEx: PiException[ObjectId]): SequenceFailure =
      SequenceFailure(Left(id), Seq((ref, null)), Seq(piEx.event))
  }

  /** Emitted by the Reducer component to log the latest PiInstance state to the PiHistory.
    * Consumed by the Sequencer component to construct up-to-date ReduceRequests.
    *
    * @param pii The latest state of the PiInstance.
    */
  case class PiiUpdate(
      pii: PiInstance[ObjectId]
  ) extends PiiHistory
      with HasPii

  /** Emitted by the Reducer to the AtomicProcess executors, assigns responsibility for
    * executing an AtomicProcess to an AtomicProcessExecutor. Uniquely identified by the
    * (PiiId, CallRef) pair.
    *
    * @param pii The PiInstance state when the Assignment was created.
    * @param callRef The ID of this call in the PiInstance.
    * @param process The name of the AtomicProcess to call.
    * @param args The value of each of the arguments to the AtomicProcess.
    */
  case class Assignment(
      pii: PiInstance[ObjectId],
      callRef: CallRef,
      process: String,
      args: Seq[PiResource]
  ) extends AnyMsg
      with HasPii {

    override def toString: String =
      s"Assignment($piiId, call ${callRef.id}, AP '$process', args $args)"
  }

  /** An output message sent to the Results topic. Has no impact on PiInstance execution,
    * but it used by `ResultListeners` to implement the `PiObservable` interface.
    *
    * @param event The `PiEvent` being logged.
    */
  case class PiiLog(event: PiEvent[ObjectId]) extends AnyMsg {
    override def piiId: ObjectId = event.id
  }
}
