package com.workflowfm.pew.stateless.instances.kafka.components

import akka._
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream.scaladsl._
import com.workflowfm.pew.stateless.components.StatelessComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.{AnyKey, AnyRes}

import scala.collection.immutable
import scala.concurrent.Future

/** Mid-Level Kafka Interface:
  * Defines the akka sources, flows, and sinks, which the high-level interface (KafkaConnectors) builds
  * executable wrappers for the StatelessComponents with.
  */
object KafkaWrapperFlows {

  import Consumer._
  import ConsumerMessage._
  import Producer._
  import ProducerMessage._
  import Subscriptions._
  import com.workflowfm.pew.stateless.StatelessMessages._

  type CMsg[V] = CommittableMessage[AnyKey, V]
  type PMsg[V] = Envelope[AnyKey, V, Committable]

  /** Wrapper type for tracking the offset to commit when a producer eventually writes
    * this object.
    */
  case class Tracked[V](
      value: V,
      partOffset: PartitionOffset
    ) {

    def part: Int = partOffset.key.partition
    def offset: Long = partOffset.offset

    def map[W]( fn: V => W ): Tracked[W]
      = Tracked( fn(value), partOffset )
  }

  def track[V]( message: TransactionalMessage[_, V] ): Tracked[V]
    = Tracked[V]( message.record.value, message.partitionOffset )

  /// KAFKA CONSUMER / AKKA SOURCES ///

  /** Kafka Consumer for the `ReduceRequest` topic.
    *
    * @return Akka source containing messages published to the `ReduceRequest` topic.
    */
  def srcReduceRequest( implicit s: KafkaExecutorSettings ): Source[Tracked[ReduceRequest], Control]
    = Transactional.source( s.csReduceRequest, topics( s.tnReduceRequest ) ).map( track )

  /** Kafka Consumer for the `Assignment` topic.
    *
    * @return Akka source containing messages published to the `Assignment` topic.
    */
  def srcAssignment( implicit s: KafkaExecutorSettings ): Source[Tracked[Assignment], Control]
    = Transactional.source( s.csAssignment, topics( s.tnAssignment ) ).map( track )

  /** Synchronous handling intended for consuming a single `PiiHistory` partition. Blocks until a ReduceRequest is
    * released by receiving both a PiiUpdate *and* at least one SequenceRequest. All SequenceRequests received are
    * responded to, even if this requires sending a ReduceRequest for a PiiUpdate that contains no SequenceRequests.
    *
    * @return Akka flow capable of sequencing a single `PiiHistory` partition into a ReduceRequest stream.
    */
  def flowSequencer: Flow[Tracked[PiiHistory], Tracked[Seq[AnyMsg]], NotUsed]
    = Flow[Tracked[PiiHistory]]
      .scan( new SequenceResponseBuilder )(_ next _)
      .map( _.response )
      .collect({ case Some( m ) => m })

  /** Kafka Consumer for the `PiiHistory` topic. Exposes the individual partition sources so they handled individually
    * by `flowPartition` argument.
    *
    * @param flowPartition Flow with which to process each partition before they are merged into the output source.
    * @return Akka source containing the processed output messages of each partition.
    */
  def srcPiiHistory( implicit s: KafkaExecutorSettings ): Source[Tracked[PiiHistory], Control]
    = Transactional.source( s.csPiiHistory, topics( s.tnPiiHistory ) ).map( track )

  /** Kafka Consumer for the `Result` topic. Configurable Group Id to allow control of the starting point
    * of consumption.
    *
    * @param groupId GroupId for the Kafka Consumer.
    * @return Akka source containing messages published to the `Results` topic.
    */
  def srcResult( groupId: String )( implicit s: KafkaExecutorSettings ): Source[Tracked[PiiResult[AnyRes]], Control]
    = Transactional.source( s.csResult withGroupId groupId, topics( s.tnResult ) ).map( track )

  /** Kafka Consumers for each topic merged into a single Akka source.
    *
    * @return Merged source for all topics.
    */
  def srcAll( implicit s: KafkaExecutorSettings ): Source[(AnyMsg, CommittableOffset), Control] = {

//    def src[V <: AnyMsg]( cs: ConsumerSettings[_, V], topic: String ): Source[Tracked[AnyMsg], Control]
//      = Transactional.source( cs, topics( topic ) ).map( track ).map( _.map( _.asInstanceOf[AnyMsg] ) )

    def src[V <: AnyMsg]( cs: ConsumerSettings[_, V], topic: String ): Source[(AnyMsg, CommittableOffset), Control]
      = Consumer.committableSource( cs, topics( topic ) )
        .map( m => (m.record.value, m.committableOffset) )

    Seq(
      src(s.csReduceRequest, s.tnReduceRequest),
      src(s.csPiiHistory, s.tnPiiHistory),
      src(s.csAssignment, s.tnAssignment),
      src(s.csResult, s.tnResult)

    ).reduce(_ merge _)
  }


  /// STREAM PROCESSING / AKKA FLOWS ///

  def flowRespond[In, Out]( component: StatelessComponent[In, Out] )
    : Flow[ Tracked[In], Tracked[Out], NotUsed ]
      = Flow[ Tracked[In] ]
        .map( _.map( component.respond ) )

  def flowRespondAll[In, Out]( component: StatelessComponent[In, Out] )
    : Flow[ Tracked[Seq[In]], Tracked[Seq[Out]], NotUsed ]
      = Flow[ Tracked[Seq[In]] ]
        .map( _.map( _.map( component.respond ) ) )

  def flowWaitFuture[T]( parallelism: Int )( implicit s: KafkaExecutorSettings )
    : Flow[ Tracked[Future[T]], Tracked[T], NotUsed ]
      = Flow[ Tracked[Future[T]] ]
        .mapAsync( parallelism )(
          msg => msg.value.map( vNew => msg.map( _ => vNew ) )( s.execCtx )
        )

  def flowCheck[T]: Flow[Tracked[T], T, NotUsed]
    = Flow[Tracked[T]]
      .map( _.value )

  def flowCheckMulti[T]: Flow[Tracked[Seq[T]], T, NotUsed]
    = Flow[Tracked[Seq[T]]]
      .map( _.value )
      .flatMapConcat( s => Source.fromIterator( () => s.iterator ) )


  /// KAFKA PRODUCER / AKKA SINKS ///

  def sinkPlain( implicit s: KafkaExecutorSettings ): Sink[AnyMsg, Future[Done]]
    = plainSink( s.psAllMessages )
      .contramap( s.record )

  def sinkTransactional( id: String )( implicit s: KafkaExecutorSettings ): Sink[Tracked[AnyMsg], Future[Done]]
    = Transactional.sink( s.psAllMessages, id )
      .contramap(
        tracked =>
          Message(
            s.record(tracked.value),
            tracked.partOffset
          )
      )

  def sinkTransactionalMulti( id: String )( implicit s: KafkaExecutorSettings ): Sink[Tracked[Seq[AnyMsg]], Future[Done]]
    = Transactional.sink( s.psAllMessages, id )
      .contramap(
        tracked =>
          MultiMessage(
            tracked
              .value
              .map( s.record )
              .to[immutable.Seq],
            tracked.partOffset
          )
      )

  /// OTHER FUNCTIONALITY ///

  def run[T]( source: Source[Tracked[T], Control], sink: Sink[Tracked[T], Future[Done]] )( implicit s: KafkaExecutorSettings ): Control
    = source
      .toMat( sink )( Keep.both )
      .mapMaterializedValue( DrainingControl.apply )  // Add shutdown control object.
      .named( this.getClass.getSimpleName )           // Name for debugging.
      .run()( s.mat )

}
