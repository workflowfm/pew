package com.workflowfm.pew.stateless.instances.kafka.components

import akka.kafka.ConsumerMessage._
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.types.ObjectId

import scala.collection.immutable

case class PartialResponse(
    pii:      Option[PiInstance[ObjectId]],
    results:  immutable.Seq[(CallRef, PiObject)],
    failures: immutable.Seq[(CallRef, Throwable)]
  ) {

  def this() = this( None, immutable.Seq(), immutable.Seq() )

  /** We only *want* to send a response when we have actionable data to send:
    * - ReduceRequest <- The latest PiInstance *and* at least one call ref to sequence.
    * - ResultFailure <- The latest PiInstance *and* all the SequenceRequests to dump.
    */
  val hasPayload: Boolean
    = pii.exists( pii =>
        if (failures.nonEmpty) {
          val haveReturned: Set[Int] = ( ( results ++ failures ) map ( _._1 ) ).map( _.id ).toSet
          pii.called.forall( haveReturned.contains )

        } else results.nonEmpty
      )

  /** @return A full message which could be built with this data, or nothing.
    */
  def message: Option[AnyMsg]
    = pii.map( pii =>
        if (failures.nonEmpty) {
          if (hasPayload) new ResultFailure( pii, failures.head._2 )
          else            SequenceFailure( pii, results, failures )

        } else ReduceRequest( pii, results )
      )

  /** Overwrite with the latest PiInstance information.
    */
  def update( newPii: PiInstance[ObjectId] ): PartialResponse
    = PartialResponse( Some( newPii ), results, failures )

  /** Update the full list of results to sequence into the next reduce.
    */
  def update( arg: (CallRef, PiObject) ): PartialResponse
    = PartialResponse( pii, arg +: results, failures )

  def merge( failure: SequenceFailure ): PartialResponse
    = PartialResponse(
      failure.pii.toOption.orElse( pii ),
      failure.results.to[immutable.Seq] ++ results,
      failure.failures.to[immutable.Seq] ++ failures
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
  * @param offset The new Kafka topic offsets after committing a response message.
  * @param responses A map of partial responses for PiInstances that need reducing.
  */
class SequenceResponseBuilder(
   val optOffset: Option[PartitionOffset],
   val responses: immutable.HashMap[ObjectId, PartialResponse],
 ) {

  type PiiT = PiInstance[ObjectId]
  type Arg = (CallRef, PiObject)

  import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows._

  /** @return An empty response builder that consumes no messages.
    */
  def this() = this( None, new immutable.HashMap )

  /** MultiMessage response for the consumed messages, or None if the
    * consumed messages lack sufficient information for a response.
    */
  val response: Option[ Tracked[ Seq[AnyMsg] ] ] =
    if ( responses.toSeq.exists( _._2.hasPayload ) ) {

    // All necessary responses:
    // - None if we lack information for a ReduceRequest
    // - Or a complete response. However, this isn't final,
    //   it may be updated when integrating new messages.
    val messages: Seq[Option[AnyMsg]]
      = responses.toSeq.map( _._2.message )

    // All PiInstances we've received information for need to be "reduced"
    // otherwise we would lose their state information when they are consumed.
    // Additionally, for performance: do not emmit empty responses.
    if ( messages.isEmpty || messages.exists( _.isEmpty ) ) None
    else Some( Tracked( messages.flatten, optOffset.get ) )

  } else None // If there is no reduce request with a payload, wait for one.

  // Helper function for updating the responses hash map.
  def update( id: ObjectId, func: PartialResponse => PartialResponse ): immutable.HashMap[ObjectId, PartialResponse]
    = responses.updated( id, func( responses.getOrElse( id, new PartialResponse ) ) )

  /** Combine 2 partition offsets of adjacent messages so they can be committed together.
    *
    * @param multiOffset Offset potentially representing multiple messages to be consumed.
    * @param singleOffset Offset representing a single message to be consumed.
    * @return A new partition offset that would consume all messsages represented by the inputs.
    */
  def combineOffsets( multiOffset: PartitionOffset, singleOffset: PartitionOffset ): PartitionOffset = {
    import math._
    require( multiOffset.key == singleOffset.key, "Messages from different partitions are being combined!" )
    require( abs( multiOffset.offset - singleOffset.offset ) == 1, "Message has been skipped!" )
    multiOffset.copy( offset = max( multiOffset.offset, singleOffset.offset ) )
  }

  /** Construct a new SequenceResponseBuilder which is responsible for
    * properly consuming an additional message.
    *
    * @param msgIn The next `CommittableMessage` to handle.
    * @return A new SequenceResponseBuilder instance.
    */
  def next( msgIn: Tracked[PiiHistory] ): SequenceResponseBuilder =

    // If we have already sent a response, start constructing the subsequent response instead.
    if (response.nonEmpty) (new SequenceResponseBuilder).next( msgIn )

    else // Otherwise begin integrating the new message into this incomplete response.
      new SequenceResponseBuilder(

        // The new message needs to be consumed with when this message commits.
        optOffset
          .map( combineOffsets( _, msgIn.partOffset ) )
          .orElse( Some( msgIn.partOffset ) ),

        // Update partial results:
        msgIn.value match {
          case msg: SequenceRequest => update( msg.piiId, _ update msg.request )
          case msg: PiiUpdate       => update( msg.pii.id, _ update msg.pii )
          case msg: SequenceFailure => update( msg.piiId, _ merge msg )
        }
      )
}