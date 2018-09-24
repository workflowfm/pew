package com.workflowfm.pew.stateless.instances.kafka.components

import akka._
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream.scaladsl._
import com.workflowfm.pew.stateless.components.StatelessComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

/** Mid-Level Kafka Interface:
  * Defines the akka sources, flows, and sinks, which the high-level interface (KafkaConnectors) builds
  * executable wrappers for the StatelessComponents with.
  *
  */
object KafkaWrapperFlows {

  import Consumer._
  import ConsumerMessage._

  import Producer._
  import ProducerMessage._

  import Subscriptions._

  import com.workflowfm.pew.stateless.StatelessMessages._

  type CMsg[V] = CommittableMessage[Any, V]
  type PMsg[V] = Envelope[Any, V, Committable]

  /** Wrapper type for tracking the offset to commit when a producer eventually writes
    * this object.
    */
  type Tracked[T] = (T, Committable)


  /// KAFKA CONSUMER / AKKA SOURCES ///

  /** Kafka Consumer for the `ReduceRequest` topic.
    *
    * @return Akka source containing messages published to the `ReduceRequest` topic.
    */
  def srcReduceRequest( implicit s: KafkaExecutorSettings ): Source[Tracked[ReduceRequest], Control]
    = committableSource( s.csReduceRequest, topics( s.tnReduceRequest ) )
      .via( flowUnwrap )

  /** Kafka Consumer for the `Assignment` topic.
    *
    * @return Akka source containing messages published to the `Assignment` topic.
    */
  def srcAssignment( implicit s: KafkaExecutorSettings ): Source[Tracked[Assignment], Control]
    = committableSource( s.csAssignment, topics( s.tnAssignment ) )
      .via( flowUnwrap )

  /** Synchronous handling intended for consuming a single `PiiHistory` partition. Blocks until a ReduceRequest is
    * released by receiving both a PiiUpdate *and* at least one SequenceRequest. All SequenceRequests received are
    * responded to, even if this requires sending a ReduceRequest for a PiiUpdate that contains no SequenceRequests.
    *
    * @return Akka flow capable of sequencing a single `PiiHistory` partition into a ReduceRequest stream.
    */
  def flowSequencer: Flow[CMsg[PiiHistory], Tracked[Seq[ReduceRequest]], NotUsed]
    = Flow[CMsg[PiiHistory]]
      .scan( new SequenceResponseBuilder )(_ next _)
      .map( _.response )
      .collect({ case Some( m ) => m })

  // TODO: FIGURE OUT HOW BEST TO HANDLE THE BREADTH PARAMETER: SHOULD BE GREATER THAN THE MAXIMUM PARTITION COUNT
  /** Kafka Consumer for the `PiiHistory` topic. Exposes the individual partition sources so they handled individually
    * by `flowPartition` argument.
    *
    * @param flowPartition Flow with which to process each partition before they are merged into the output source.
    * @return Akka source containing the processed output messages of each partition.
    */
  def srcPiiHistory[Out]( flowPartition: Flow[CMsg[PiiHistory], Out, NotUsed] )( implicit s: KafkaExecutorSettings )
    : Source[Out, Control] =
      committablePartitionedSource( s.csPiiHistory, topics( s.tnPiiHistory ) )
      .map( _._2 )
      .flatMapMerge[Out, NotUsed]( 4, _.map( _.asInstanceOf[CMsg[PiiHistory]] ) via flowPartition )

  /** Kafka Consumer for the `Result` topic. Configurable Group Id to allow control of the starting point
    * of consumption.
    *
    * @param groupId GroupId for the Kafka Consumer.
    * @return Akka source containing messages published to the `Results` topic.
    */
  def srcResult( groupId: String )( implicit s: KafkaExecutorSettings ): Source[Tracked[PiiResult[Any]], Control]
    = committableSource( s.csResult withGroupId groupId, topics( s.tnResult ) )
      .via( flowUnwrap )

  /** Kafka Consumers for each topic merged into a single Akka source.
    *
    * @return Merged source for all topics.
    */
  def srcAll( implicit s: KafkaExecutorSettings ): Source[Tracked[AnyMsg], Control]
    = Seq(
      committableSource( s.csReduceRequest, topics( s.tnReduceRequest ) ) via flowUnwrap,
      committableSource( s.csPiiHistory, topics( s.tnPiiHistory ) ) via flowUnwrap,
      committableSource( s.csAssignment, topics( s.tnAssignment ) ) via flowUnwrap,
      committableSource( s.csResult, topics( s.tnResult ) ) via flowUnwrap,

    ) reduce (_ merge _)


  /// STREAM PROCESSING / AKKA FLOWS ///

  def flowUnwrap[K, V]: Flow[CommittableMessage[K, V], Tracked[V], NotUsed]
    = Flow[CommittableMessage[K, V]]
      .map( msg => (msg.record.value, msg.committableOffset) )

  def flowRespond[In, Out]( component: StatelessComponent[In, Out] )
    : Flow[ Tracked[In], Tracked[Out], NotUsed ]
      = Flow[ Tracked[In] ]
        .map({
          case (message, offset) =>
            ( component.respond( message ), offset )
        })

  def flowRespondAll[In, Out]( component: StatelessComponent[In, Out] )
    : Flow[ Tracked[Seq[In]], Tracked[Seq[Out]], NotUsed ]
      = Flow[ Tracked[Seq[In]] ]
      .map({
        case (messages, offset) =>
          ( messages.map( component.respond(_) ), offset )
      })

  def flowWaitFuture[T]( parallelism: Int )( implicit s: KafkaExecutorSettings )
    : Flow[ Tracked[Future[T]], Tracked[T], NotUsed ]
      = Flow[ Tracked[Future[T]] ]
      .mapAsync( parallelism )({
        case ( message, offset ) =>
          message.map( ( _, offset ) )( s.execCtx )
      })

  def flowMessage( implicit s: KafkaExecutorSettings )
    : Flow[ Tracked[AnyMsg], PMsg[AnyMsg], NotUsed ]
      = Flow[ Tracked[AnyMsg] ]
        .map({
          case (message, offset) =>
            Message( s.record( message ), offset )
        })

  def flowMultiMessage( implicit s: KafkaExecutorSettings )
    : Flow[ Tracked[Seq[AnyMsg]], PMsg[AnyMsg], NotUsed ]
      = Flow[ Tracked[Seq[AnyMsg]] ]
        .map({
          case ( messages, offset ) =>
            MultiMessage( messages.map( s.record ).to, offset )
        })


  /// KAFKA PRODUCER / AKKA SINKS ///

  def sinkPlain( implicit s: KafkaExecutorSettings ): Sink[AnyMsg, Future[Done]]
    = plainSink( s.psAllMessages )
      .contramap( s.record )

  def sinkProducerMsg( implicit s: KafkaExecutorSettings ): Sink[PMsg[AnyMsg], Future[Done]]
    = commitableSink( s.psAllMessages )


  /// OTHER FUNCTIONALITY ///

  def run[T]( source: Source[T, Control], sink: Sink[T, Future[Done]] )( implicit s: KafkaExecutorSettings ): Control
    = source
      .toMat( sink )( Keep.both )
      .mapMaterializedValue( DrainingControl.apply )  // Add shutdown control object.
      .named( this.getClass.getSimpleName )           // Name for debugging.
      .run()( s.materializer )

}
