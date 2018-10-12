package com.workflowfm.pew.stateless.instances.kafka.components

import akka.Done
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch, PartitionOffset}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.kafka._
import akka.stream.scaladsl.{Sink, Source}
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId

import scala.concurrent.{ExecutionContext, Future}

sealed abstract class Tracked[Value] {
  def value: Value
  def part: Int

  protected def map[NewValue]( fn: Value => NewValue ): Tracked[NewValue]

  /** Combine 2 partition offsets of adjacent messages so they can be committed together.
    *
    * @param batchOffset Offset potentially representing multiple messages to be consumed.
    * @param singleOffset Offset representing a single message to be consumed.
    * @return A new partition offset that would consume all messsages represented by the inputs.
    */
  protected def fold[NewValue]( fn: (Value, NewValue) => Value )( other: Tracked[NewValue] ): Tracked[Value]
}

object Tracked {

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

case class CommitTracked[Value](
    value: Value,
    part: Int,
    commit: Committable
  ) extends Tracked[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    require( other.isInstanceOf[CommitTracked[NewValue]] )

    val that = other.asInstanceOf[CommitTracked[NewValue]]
    require( that.commit.isInstanceOf[CommittableOffset] )

    copy(
      value = fn( value, that.value ),
      commit = ( commit match {
        case offset: CommittableOffset => ConsumerMessage.emptyCommittableOffsetBatch.updated( offset )
        case batch: CommittableOffsetBatch => batch

      }).updated( that.commit.asInstanceOf[CommittableOffset] )
    )
  }
}

object CommitTracked {

  def source[Value]( cs: ConsumerSettings[_, Value], sub: Subscription ): Source[CommitTracked[Value], Control]
    = Consumer.committableSource( cs, sub )
      .map( msg =>
        CommitTracked(
          msg.record.value, 0,
          msg.committableOffset
        )
      )

  def sourcePartitioned[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[CommitTracked[Value], Control]
    = Consumer.committablePartitionedSource( cs, sub )
      .flatMapMerge( Int.MaxValue, {
        case (topicPartition, source) =>
          source.map( msg =>
            CommitTracked(
              msg.record.value,
              topicPartition.partition(),
              msg.committableOffset
            )
          )
      })

  def sink[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[CommitTracked[V], Future[Done]]
    = Producer.commitableSink( s.psAllMessages )
      .contramap( msg =>
        ProducerMessage.Message(
          s.record( msg.value ),
          msg.commit
        )
      )

  def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorSettings ): Sink[CommitTracked[Seq[V]], Future[Done]]
    = Producer.commitableSink( s.psAllMessages )
      .contramap( msgs =>
        ProducerMessage.MultiMessage(
          msgs.value.map(s.record).to,
          msgs.commit
        )
      )

}

case class Transaction[Value](
    value: Value,
    partOffset: PartitionOffset
  ) extends Tracked[Value] {

  def offset: Long = partOffset.offset

  override def part: Int = partOffset.key.partition

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    assert( false, "This function should work, but causes hangs when used with Transactional.sink" )

    require( other.isInstanceOf[Transaction[NewValue]] )
    val that = other.asInstanceOf[Transaction[NewValue]]

    require( partOffset.key == that.partOffset.key)
    copy(
      value = fn( value, that.value ),
      partOffset = partOffset.copy( offset = Math.max( offset, that.offset ) )
    )
  }
}

object Transaction {

  def source[Value]( cs: ConsumerSettings[_, Value], sub: Subscription ): Source[Transaction[Value], Control]
    = Transactional.source( cs, sub )
      .map( msg => Transaction( msg.record.value(), msg.partitionOffset ) )

  def sink[V <: AnyMsg]( transId: String )( implicit s: KafkaExecutorSettings ): Sink[Transaction[V], Future[Done]]
    = {
    val id = ObjectId.get.toString
    Transactional.sink( s.psAllMessages, id )
      .contramap( msg =>
        ProducerMessage.Message(
          s.record( msg.value ),
          msg.partOffset
        )
      )
  }

  def sinkMulti[V <: AnyMsg]( transId: String )( implicit s: KafkaExecutorSettings ): Sink[Transaction[Seq[V]], Future[Done]]
    = {
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