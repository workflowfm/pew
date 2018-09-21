package com.workflowfm.pew.stateless.instances.kafka.components

import akka.kafka.ConsumerMessage._
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.types.ObjectId

import scala.collection.immutable

case class PartialResponse(
    pii:    Option[PiInstance[ObjectId]],
    args:   immutable.Seq[(CallRef, PiObject)]
  ) {

  def this() = this( None, immutable.Seq() )

  /** We only *want* to send a response when there is a valid payload.
    */
  val hasPayload: Boolean = args.nonEmpty

  /** @return Optional ReduceRequest which could be built from this data.
    */
  def request: Option[ReduceRequest]
    = pii.map( ReduceRequest( _, args ) )

  /** Overwrite with the latest PiInstance information.
    */
  def update( newPii: PiInstance[ObjectId] )
    = PartialResponse( Some( newPii ), args )

  /** Update the full list of results to sequence into the next reduce.
    */
  def update( arg: (CallRef, PiObject) )
    = PartialResponse( pii, args :+ arg )
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
   val offset: CommittableOffsetBatch,
   val responses: immutable.HashMap[ObjectId, PartialResponse],
 ) {

  type PiiT = PiInstance[ObjectId]
  type Arg = (CallRef, PiObject)

  import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows._

  /** @return An empty response builder that consumes no messages.
    */
  def this() = this( emptyCommittableOffsetBatch, new immutable.HashMap )

  /** MultiMessage response for the consumed messages, or None if the
    * consumed messages lack sufficient information for a response.
    */
  val response: Option[ Tracked[ Seq[ReduceRequest] ] ] =
    if ( responses.toSeq.exists( _._2.hasPayload ) ) {

    // All necessary responses:
    // - None if we lack information for a ReduceRequest
    // - Or a complete response. However, this isn't final,
    //   it may be updated when integrating new messages.
    val messages: Seq[Option[ReduceRequest]]
      = responses.toSeq.map( _._2.request )

    // All PiInstances we've received information for need to be "reduced"
    // otherwise we would lose their state information when they are consumed.
    // Additionally, for performance: do not emmit empty responses.
    if ( messages.isEmpty || messages.exists( _.isEmpty ) ) None
    else Some( messages.flatten, offset )

  } else None // If there is no reduce request with a payload, wait for one.

  // Helper function for updating the responses hash map.
  def update( id: ObjectId, func: PartialResponse => PartialResponse ): immutable.HashMap[ObjectId, PartialResponse]
    = responses.updated( id, func( responses.getOrElse( id, new PartialResponse ) ) )

  /** Construct a new SequenceResponseBuilder which is responsible for
    * properly consuming an additional message.
    *
    * @param msgIn The next `CommittableMessage` to handle.
    * @return A new SequenceResponseBuilder instance.
    */
  def next( msgIn: CMsg[PiiHistory] ): SequenceResponseBuilder =

    // If we have already sent a response, start constructing the subsequent response instead.
    if (response.nonEmpty) (new SequenceResponseBuilder).next( msgIn )

    else // Otherwise begin integrating the new message into this incomplete response.
      new SequenceResponseBuilder(

        // The new message needs to be consumed with when this message commits.
        offset.updated( msgIn.committableOffset ),

        // Update partial results:
        msgIn.record.value match {
          case msg: SequenceRequest => update( msg.piiId, _.update( msg.request ) )
          case msg: PiiUpdate       => update( msg.pii.id, _.update( msg.pii ) )
        }
      )
}