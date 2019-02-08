package com.workflowfm.pew.stateless

import akka.actor.ActorSystem
import com.workflowfm.pew.execution.RexampleTypes._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonKafkaExecutorSettings, KafkaCodecRegistry}
import com.workflowfm.pew.stateless.instances.kafka.{CompleteKafkaExecutor, CustomKafkaExecutor}
import com.workflowfm.pew.util.ClassMap
import com.workflowfm.pew.{PiEvent, PiProcessStore, SimpleProcessStore}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

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

    def fail(): Unit = {
      promise.failure( new Exception("PcI: Test Failure" ) )
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

  def newSettings( piStore: PiProcessStore ): BsonKafkaExecutorSettings
    = new BsonKafkaExecutorSettings( new KafkaCodecRegistry( piStore ), system, executionContext )

  class ProcessType( val store: PiProcessStore) extends Object {
    val settings: BsonKafkaExecutorSettings = newSettings( store )
  }

  lazy val completeProcess: ProcessType
    = new ProcessType( SimpleProcessStore(
      pai, pbi, pci, pci2,
      ri, // ri2, rif,
      failp
    ))

  lazy val failureProcess: ProcessType
    = new ProcessType( SimpleProcessStore(
      pai, pbi, pcif, pci2,
      ri, ri2, rif,
      failp
    ))

  lazy val shutdownProcess: ProcessType
    = new ProcessType( SimpleProcessStore(
      pai, pbi, pciw, pci2,
      ri, ri2, rif,
      failp
    ))


  def makeExecutor( settings: KafkaExecutorSettings ): CustomKafkaExecutor = {
    CompleteKafkaExecutor[(Y, Z)]( settings )
  }

  // val isPiiResult: AnyMsg => Boolean = _.isInstanceOf[PiiResult[_]]

  // TODO: Fix consumer shutdown: https://github.com/akka/alpakka-kafka/issues/166
  def outstanding( consume: Boolean ): Seq[ AnyMsg ] = {
    implicit val s: KafkaExecutorSettings = completeProcess.settings

    if (consume) println("!!! CONSUMING OUTSTANDING MESSAGES !!!")

    val fOutstanding: Future[Seq[AnyMsg]]
      = KafkaWrapperFlows.srcAll
        .wireTap( msg =>
          if (consume)  msg.commit.commitScaladsl()
          else          ()
        )
        .map( _.value )
        .completionTimeout(5.seconds)
        .map( Some(_) )
        .recover({ case _: TimeoutException => None })
        .collect({ case Some( msg ) => msg })
        .runFold( Seq(): Seq[AnyMsg] )( _ :+ _ )( s.mat )

    Await.result( fOutstanding, Duration.Inf )
  }

  type MessageMap = ClassMap[AnyMsg]
  type LogMap = ClassMap[PiEvent[_]]

  class MessageDrain( consume: Boolean = false )
    extends MessageMap( outstanding( consume ) )

  def toLogMap( msgMap: MessageMap ): LogMap
    = new ClassMap[PiEvent[_]]( msgMap[PiiLog] map (_.event) )

}
