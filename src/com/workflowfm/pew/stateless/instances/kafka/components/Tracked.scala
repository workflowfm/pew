package com.workflowfm.pew.stateless.instances.kafka.components

import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch, PartitionOffset}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId

import scala.concurrent.{ExecutionContext, Future}

abstract class Tracked[Value] {
  def value: Value

  protected def map[NewValue]( fn: Value => NewValue ): Tracked[NewValue]

  /** Combine 2 partition offsets of adjacent messages so they can be committed together.
    *
    * @param batchOffset Offset potentially representing multiple messages to be consumed.
    * @param singleOffset Offset representing a single message to be consumed.
    * @return A new partition offset that would consume all messsages represented by the inputs.
    */
  protected def fold[NewValue]( fn: (Value, NewValue) => Value )( other: Tracked[NewValue] ): Tracked[Value]
}

trait TrackedSource[ T[X] <: Tracked[X] ] {
  implicit val trackedSource: TrackedSource[T] = this
  def source[V]( cs: ConsumerSettings[_, V], sub: AutoSubscription ): Source[T[V], Control]
}

trait TrackedSink[ T[X] <: Tracked[X] ] {
  implicit val trackedSink: TrackedSink[T] = this
  def sink[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[T[V], Future[Done]]
}

trait TrackedMultiSink[ T[X] <: Tracked[X] ] {
  implicit val trackedMultiSink: TrackedMultiSink[T] = this
  def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[T[Seq[V]], Future[Done]]
}

object Tracked {

  def source[T[X] <: Tracked[X], V]
    ( cs: ConsumerSettings[_, V], sub: AutoSubscription )
    ( implicit s: KafkaExecutorSettings, ts: TrackedSource[T] )
    : Source[T[V], Control] = ts.source( cs, sub )

  def sink[T[X] <: Tracked[X], V <: AnyMsg]( implicit s: KafkaExecutorSettings, ts: TrackedSink[T] ): Sink[T[V], Future[Done]]
    = ts.sink( s )

  def sinkMulti[T[X] <: Tracked[X], V <: AnyMsg]( implicit s: KafkaExecutorSettings, ts: TrackedMultiSink[T] ): Sink[T[Seq[V]], Future[Done]]
    = ts.sinkMulti( s )

  def fmap[T[X] <: Tracked[X], In, Out]( fn: In => Out )( trackedIn: T[In] ): T[Out]
    = trackedIn.map( fn ).asInstanceOf[T[Out]]

  def freplace[T[X] <: Tracked[X], In, Out]( tracked: T[In] )( newValue: Out ): T[Out]
    = fmap( (_: In) => newValue )( tracked )

  def exposeFuture[V, T[X] <: Tracked[X]]( fut: T[Future[V]] )( implicit exec: ExecutionContext ): Future[T[V]]
    = fut.value.map( freplace( fut ) )( exec )

  def flatten[Msg, T[X] <: Tracked[X]]( consuming: Seq[T[Msg]] ): T[Seq[Msg]] = {
    require( consuming.nonEmpty )

    val foldStart: T[Seq[Msg]] = fmap[T, Msg, Seq[Msg]]( Seq(_) )( consuming.head )

    def combine( tseq: T[Seq[Msg]], tmsg: T[Msg] ): T[Seq[Msg]]
      = tseq.fold[Msg]( _ :+ _ )( tmsg ).asInstanceOf[ T[Seq[Msg]] ]

    consuming.tail.foldLeft[T[Seq[Msg]]]( foldStart )( combine )
  }
}

trait HasCommittable[Value] extends Tracked[Value] {
  def commit: Committable
}

trait HasCommittableSinks[T[X] <: HasCommittable[X]]
  extends TrackedSink[T]
  with TrackedMultiSink[T] {

  override def sink[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[HasCommittable[V], Future[Done]]
    = Producer.commitableSink( s.psAllMessages )
      .contramap( msg =>
        ProducerMessage.Message(
          s.record( msg.value ),
          msg.commit
        )
      )

  override def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[HasCommittable[Seq[V]], Future[Done]]
    = Producer.commitableSink( s.psAllMessages )
      .contramap( msgs =>
        ProducerMessage.MultiMessage(
          msgs.value.map(s.record).to,
          msgs.commit
        )
      )
}

case class CommitTracked[Value](
    value: Value,
    commit: Committable
  ) extends Tracked[Value] with HasCommittable[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy(
      value = fn( value, other.value ),
      commit = CommitTracked.unsafeMerge( commit, other.asInstanceOf[CommitTracked[_]].commit )
    )
}

object CommitTracked
  extends TrackedSource[CommitTracked]
  with HasCommittableSinks[CommitTracked] {

  def unsafeMerge( left: Committable, right: Committable ): Committable = {
    require( right.isInstanceOf[CommittableOffset] )

    ( left match {
      case offset: CommittableOffset => ConsumerMessage.emptyCommittableOffsetBatch.updated( offset )
      case batch: CommittableOffsetBatch => batch
    }).updated( right.asInstanceOf[CommittableOffset] )
  }

  def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[CommitTracked[Value], Control]
    = Consumer.committableSource( cs, sub )
      .map( msg =>
        CommitTracked(
          msg.record.value,
          msg.committableOffset
        )
      )

}

trait HasPartition[Value]
  extends Tracked[Value] {

  def part: Int
}

case class PartTracked[Value](
   value: Value,
   commit: Committable,
   part: Int

  ) extends Tracked[Value]
  with HasCommittable[Value]
  with HasPartition[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy(
      value = fn( value, other.value ),
      commit = CommitTracked.unsafeMerge( commit, other.asInstanceOf[PartTracked[_]].commit )
    )
}

object PartTracked
  extends TrackedSource[PartTracked]
  with HasCommittableSinks[PartTracked] {

  def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[PartTracked[Value], Control]
    = Consumer.committablePartitionedSource( cs, sub )
      .flatMapMerge( Int.MaxValue, {
        case (topicPartition, source) =>
          println(s"Started consuming partition $topicPartition.")
          source.map( msg =>
            PartTracked(
              msg.record.value,
              msg.committableOffset,
              topicPartition.partition()
            )
          )
      })

}

case class Transaction[Value](
    value: Value,
    partOffset: PartitionOffset

  ) extends Tracked[Value] with HasPartition[Value] {

  def offset: Long = partOffset.offset

  override def part: Int = partOffset.key.partition

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    assert( false, "This function should work, but causes hangs when used with Transactional.sink" )

    require( other.isInstanceOf[Transaction[NewValue]] )
    val that = other.asInstanceOf[Transaction[NewValue]]

    require( partOffset.key == that.partOffset.key, "Trying to merge offsets of different partitions." )
    require( partOffset.offset + 1 == that.partOffset.offset, "Skipping Consumer message." )
    copy(
      value = fn( value, that.value ),
      partOffset = partOffset.copy( offset = Math.max( offset, that.offset ) )
    )
  }
}

object Transaction
  extends TrackedSource[Transaction]
  with TrackedSink[Transaction]
  with TrackedMultiSink[Transaction] {

  implicit val meh: TrackedSource[Transaction] = this
  implicit val meh2: TrackedSink[Transaction] = this
  implicit val meh3: TrackedMultiSink[Transaction] = this

  override def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[Transaction[Value], Control]
    = Transactional.source( cs, sub )
      .map( msg => Transaction( msg.record.value(), msg.partitionOffset ) )

  override def sink[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[Transaction[V], Future[Done]] = {
    val id = ObjectId.get.toString
    Transactional.sink( s.psAllMessages, id )
    .contramap( msg =>
      ProducerMessage.Message(
        s.record( msg.value ),
        msg.partOffset
      )
    )
  }

  override def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[Transaction[Seq[V]], Future[Done]] = {
    val id = ObjectId.get.toString
    Transactional.sink( s.psAllMessages, id )
    .contramap( msgs =>
      ProducerMessage.MultiMessage(
        msgs.value.map(s.record).to,
        msgs.partOffset
      )
    )
  }
}

case class Untracked[Value](
   value: Value
 ) extends Tracked[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy( value = fn( value, other.value ) )
}

object Untracked
  extends TrackedSource[Untracked]
  with TrackedSink[Untracked] {

  def source[Value]( messages: Seq[Value] ): Source[Untracked[Value], NotUsed]
    = Source.fromIterator( () => messages.iterator ).map( Untracked(_) )

  override def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[Untracked[Value], Control]
    = Consumer.committableSource( cs, sub )
      .mapAsync(1)( message =>
        message
        .committableOffset
        .commitScaladsl()
        .map( _ => Untracked( message.record.value ) )( ExecutionContext.global )
      )

  override def sink[Value <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[Untracked[Value], Future[Done]]
    = Producer.plainSink( s.psAllMessages )
      .contramap( (msg: Untracked[Value]) => s.record( msg.value ) )

}

case class MockTransaction[Value](
    value: Value,
    part: Int,
    minOffset: Long,
    maxOffset: Long

  ) extends Tracked[Value] with HasPartition[Value] {

  def this( value: Value, part: Int, offset: Long )
    = this( value, part, offset, offset + 1 )

  def consuming: Long = maxOffset - minOffset

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    require( other.isInstanceOf[MockTransaction[NewValue]] )
    val that: MockTransaction[NewValue] = other.asInstanceOf[MockTransaction[NewValue]]

    // Strict checks to emulate requirements merging requirements of real Transactions.
    require( part == that.part, s"Cannot merge messages on different partitions" )
    require( that.minOffset + 1 == that.maxOffset, s"Target message cannot already be aggregated!" )
    require( minOffset < maxOffset, s"Messages must be consumed in chronological order!" )
    require( that.minOffset < that.maxOffset, s"Messages must be consumed in chronological order!" )
    require( that.minOffset == maxOffset, s"Messages cannot be skipped! ($maxOffset -> ${that.minOffset})" )

    copy( value = fn( value, that.value ), maxOffset = that.maxOffset )
  }
}

object MockTransaction {

  def source[Value]( messages: Seq[(Value, Int)] ): Source[MockTransaction[Value], NotUsed]
    = Source
      .fromIterator( () => messages.iterator )
      .groupBy( Int.MaxValue, _._2 )
      .zip( Source(1 to Int.MaxValue) )
      .map({ case ((msg, part), i) => new MockTransaction( msg, part, i ) })
      .mergeSubstreams

}

case class MockTracked[Value](
    value: Value,
    part: Int,
    consuming: Long

  ) extends Tracked[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    require( other.isInstanceOf[MockTracked[NewValue]] )
    val that: MockTracked[NewValue] = other.asInstanceOf[MockTracked[NewValue]]

    copy( value = fn( value, that.value ), consuming = consuming + that.consuming )
  }
}

object MockTracked {

  def apply[T]( mock: MockTransaction[T] ): MockTracked[T]
    = MockTracked( mock.value, mock.part, mock.maxOffset - mock.minOffset )

  def source[Value]( messages: Seq[(Value, Int)] ): Source[MockTracked[Value], NotUsed]
    = Source
      .fromIterator( () => messages.iterator )
      .map({ case (v, p) => MockTracked(v, p, 1) })

}

