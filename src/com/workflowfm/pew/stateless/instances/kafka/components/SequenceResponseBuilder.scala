package com.workflowfm.pew.stateless.instances.kafka.components

import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.{ PiEventCallEnd, PiFailure, PiInstance, PiObject }
import org.bson.types.ObjectId

import scala.collection.immutable

case class PartialResponse(
    pii: Option[PiInstance[ObjectId]],
    returns: immutable.Seq[CallResult],
    errors: immutable.Seq[PiFailure[ObjectId]]
) {

  def this() = this(None, immutable.Seq(), immutable.Seq())

  /** All the AtomicProcess calls that are known to be concluded within this response.
    */
  lazy val finishedCalls: Set[Int] = {
    val returnedCalls = returns.map(_._1.id)
    val crashedCalls = errors.collect { case event: PiEventCallEnd[_] => event.ref }
    (returnedCalls ++ crashedCalls).toSet
  }

  /** We only *want* to send a response when we have actionable data to send:
    * - ResultFailure <- The latest PiInstance *and* all the SequenceRequests to dump.
    * - ReduceRequest <- The latest PiInstance *and* at least one call ref to sequence.
    */
  val hasPayload: Boolean = pii.exists(pii =>
    if (errors.nonEmpty)
      pii.called.forall(finishedCalls.contains)
    else
      returns.nonEmpty
  )

  val cantSend: Boolean = pii.isEmpty

  /** @return A full message which could be built with this data, or nothing.
    */
  def message: Seq[AnyMsg] = pii
    .map(pii =>
      if (errors.nonEmpty) {
        if (hasPayload) Seq(PiiLog(PiFailure.condense(errors)))
        else Seq(SequenceFailure(Right(pii), returns, errors))
      } else Seq(ReduceRequest(pii, returns))
    )
    .getOrElse(Seq())

  /** Overwrite with the latest PiInstance information.
    */
  def update(newPii: PiInstance[ObjectId]): PartialResponse =
    PartialResponse(Some(newPii), returns, errors)

  /** Update the full list of results to sequence into the next reduce.
    */
  def update(arg: (CallRef, PiObject)): PartialResponse =
    PartialResponse(pii, arg +: returns, errors)

  def merge(failure: SequenceFailure): PartialResponse = PartialResponse(
    failure.pii.toOption.orElse(pii),
    failure.returns.to[immutable.Seq] ++ returns,
    failure.errors.to[immutable.Seq] ++ errors
  )
}

/** Object that aggregates information required to properly respond to a sequence
  * of PiiUpdate and SequenceRequest messages in a combined SequenceRequest topic.
  *
  * Example Sequence and Responses:
  * older...                                         ...newer
  * [ S1 S1 U1|U1 S2 S1 U2|S1 U2 S2 U1|S2 U1 U2|U1 U2 U3 S3 ] pii history topic
  * [    R1   |   R1 R2   |   R1 R2   | R1 R2  |  R1 R2 R3  ] reduce request topic
  *
  * @param consuming The Consumer messages to be consumed by the next output.
  * @param responses A map of partial responses for PiInstances that need reducing.
  */
class SequenceResponseBuilder[T[X] <: Tracked[X]](
    val consuming: immutable.List[T[PiiHistory]],
    val responses: immutable.HashMap[ObjectId, PartialResponse]
) {

  type PiiT = PiInstance[ObjectId]
  type Arg = (CallRef, PiObject)

  /** @return An empty response builder that consumes no messages.
    */
  def this() = this(List(), new immutable.HashMap)

  /** MultiMessage response for the consumed messages, or None if the
    * consumed messages lack sufficient information for a response.
    */
  val response: Option[T[Seq[AnyMsg]]] =
    if (responses.toSeq.exists(_._2.hasPayload)) {

      // All necessary responses:
      // - None if we lack information for a ReduceRequest
      // - Or a complete response. However, this isn't final,
      //   it may be updated when integrating new messages.
      lazy val allMessages: Seq[AnyMsg] = responses.values.flatMap(_.message).toSeq

      // All PiInstances we've received information for need to be "reduced"
      // otherwise we would lose their state information when they are consumed.
      // Additionally, for performance: do not emmit empty responses.
      if (responses.values.exists(_.cantSend) || allMessages.isEmpty) None
      else Some(Tracked.freplace(Tracked.flatten(consuming.reverse))(allMessages))

    } else None // If there is no reduce request with a payload, wait for one.

  // Helper function for updating the responses hash map.
  def update(
      id: ObjectId,
      func: PartialResponse => PartialResponse
  ): immutable.HashMap[ObjectId, PartialResponse] =
    responses.updated(id, func(responses.getOrElse(id, new PartialResponse)))

  /** Construct a new SequenceResponseBuilder which is responsible for
    * properly consuming an additional message.
    *
    * @param msgIn The next `CommittableMessage` to handle.
    * @return A new SequenceResponseBuilder instance.
    */
  def next(msgIn: T[PiiHistory]): SequenceResponseBuilder[T] =
    // If we have already sent a response, start constructing the subsequent response instead.
    if (response.nonEmpty)
      (new SequenceResponseBuilder).next(msgIn)
    else // Otherwise begin integrating the new message into this incomplete response.
      new SequenceResponseBuilder(
        // The new message needs to be consumed with when this message commits.
        msgIn :: consuming,
        // Update partial results:
        update(
          msgIn.value.piiId,
          msgIn.value match {
            case msg: SequenceRequest => _ update msg.request
            case msg: PiiUpdate => _ update msg.pii
            case msg: SequenceFailure => _ merge msg
          }
        )
      )
}
