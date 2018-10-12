package com.workflowfm.pew.stateless

import akka.actor.ActorSystem
import com.workflowfm.pew.execution.RexampleTypes.{R, Y}
import com.workflowfm.pew.execution._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.components.{KafkaWrapperFlows, Tracked, Transaction}
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonKafkaExecutorSettings, KafkaCodecRegistry}
import com.workflowfm.pew.stateless.instances.kafka.{CompleteKafkaExecutor, MinimalKafkaExecutor}
import com.workflowfm.pew.{PiProcessStore, SimpleProcessStore}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.reflect.ClassTag

trait KafkaTests extends ProcessExecutorTester {

  import akka.Done
  import com.workflowfm.pew.execution.RexampleTypes.{B, Pc, Z}

  import scala.concurrent.duration.Duration
  import scala.concurrent.{Await, Promise}

  class PcIWait( s: String = "PcI" ) extends Pc {
    override def iname: String = s

    private var promise: Promise[Done] = Promise[Done]()

    override def apply( arg0 :B ) :Z = {
      Await.result( promise.future, Duration.Inf )
      iname + "SleptFor" + arg0 +"s"
    }

    def continue(): Unit = {
      promise.success( Done )
      promise = Promise[Done]()
    }
  }

  val failp = new FailP
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val pci2 = new PcI("PcX")
  val pcif = new PcIF
  val pciw = new PcIWait
  val ri = new R(pai,pbi,pci)
  val ri2 = new R(pai,pbi,pci2)
  val rif = new R(pai,pbi,pcif)

  implicit val system: ActorSystem = ActorSystem("AkkaExecutorTests")
  implicit val executionContext: ExecutionContext = ExecutionContext.global //sys

  val newSettings: PiProcessStore => KafkaExecutorSettings = piStore =>
    new BsonKafkaExecutorSettings( new KafkaCodecRegistry( piStore ), system, executionContext )

  val completeProcessStore
    = SimpleProcessStore(
      pai, pbi, pci, pci2,
      ri, ri2, rif,
      failp
    )

  val failureProcessStore
    = SimpleProcessStore(
      pai, pbi, pcif, pci2,
      ri, ri2, rif,
      failp
    )

  val shutdownProcessStore
    = SimpleProcessStore(
      pai, pbi, pciw, pci2,
      ri, ri2, rif,
      failp
    )


  def makeExecutor(store: SimpleProcessStore): MinimalKafkaExecutor[(Y, Z)] = {
    CompleteKafkaExecutor[(Y, Z)]( newSettings( store ) )
  }

  val isPiiResult: AnyMsg => Boolean = _.isInstanceOf[PiiResult[_]]

  // TODO: Fix consumer shutdown: https://github.com/akka/alpakka-kafka/issues/166
  def outstanding( consume: Boolean ): Seq[ AnyMsg ] = {
    implicit val s: KafkaExecutorSettings = newSettings( completeProcessStore )

    if (consume) println("!!! CONSUMING OUTSTANDING MESSAGES !!!")

    val fOutstanding: Future[Seq[AnyMsg]] =
      ( if (consume)
          KafkaWrapperFlows.srcAll
          .wireTap(
            Transaction.sinkMulti("OutstandingConsumer")
            .contramap[Transaction[AnyMsg]]( Tracked.freplace(_)(Seq()) )
          )
      else KafkaWrapperFlows.srcAll )
        .map( _.value )
        .completionTimeout(5.seconds)
        .map( Some(_) )
        .recover({ case _: TimeoutException => None })
        .collect({ case Some( msg ) => msg })
        .runFold( Seq(): Seq[AnyMsg] )( _ :+ _ )( s.mat )

    Await.result( fOutstanding, Duration.Inf )
  }

  def isAllTidy( isValid: AnyMsg => Boolean = isPiiResult ): Boolean
    = outstanding( true ).forall( isValid )

  class MessageDrain( consume: Boolean = false ) {

    val msgMap: Map[Class[_], Seq[AnyMsg]] =
      outstanding( consume )
        .groupBy[Class[_]]( _.getClass )
        .withDefaultValue[Seq[AnyMsg]]( Seq() )

    def apply[T]( implicit ct: ClassTag[T] ): Seq[T]
      = msgMap( ct.runtimeClass ).asInstanceOf[Seq[T]]
  }

}
