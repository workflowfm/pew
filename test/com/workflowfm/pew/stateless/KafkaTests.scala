package com.workflowfm.pew.stateless

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.workflowfm.pew.{PiProcessStore, SimpleProcessStore}
import com.workflowfm.pew.execution.RexampleTypes.{R, Y}
import com.workflowfm.pew.execution._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.MinimalKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonKafkaExecutorSettings, KafkaCodecRegistry}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.reflect.ClassTag
import scala.concurrent.duration._

trait KafkaTests extends ProcessExecutorTester {

  import akka.Done
  import com.workflowfm.pew.execution.RexampleTypes.{B, Pc, Z}

  import scala.concurrent.{Await, Promise}
  import scala.concurrent.duration.Duration

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
      pai, pbi, pci, pci2,
      ri, ri2, rif,
      failp
    )

  val shutdownProcessStore
    = SimpleProcessStore(
      pai, pbi, pciw, pci2,
      ri, ri2, rif,
      failp
    )

  val errors: mutable.Queue[String] = mutable.Queue()
  val sentUniques: mutable.Set[Any] = mutable.Set()

  def uniques: AnyMsg => Seq[Any] = {
    case m: PiiUpdate           => Seq() // these are allowed to be duplicated.
    case m: SequenceRequest     => Seq( ( "seqreq", m.piiId, m.request._1.id ) )
    case m: ReduceRequest       => m.args.map( r => ("redreq", m.pii.id, r._1.id ) )
    case m: Assignment          => Seq( ("ass", m.pii.id, m.callRef.id ) )
    case m: PiiResult[_]        => Seq( ("res", m.pii.id ) )
  }

  def makeExecutor(store: SimpleProcessStore): MinimalKafkaExecutor[(Y, Z)] = {
    implicit val s: KafkaExecutorSettings = newSettings( store )

    TestKafkaExecutor[(Y, Z)]( Sink.foreach[AnyMsg] {
      message => {
        System.out.println(s"Sending: $message")

        val keys = uniques(message)
        keys.filter( sentUniques.contains ).foreach( u => errors += s"Duplicate unique '$u'." )
        keys.foreach( sentUniques.add )
      }
    })
  }

  val isPiiResult: AnyMsg => Boolean = _.isInstanceOf[PiiResult[_]]

  // TODO: Fix consumer shutdown: https://github.com/akka/alpakka-kafka/issues/166
  def outstanding( consume: Boolean ): Seq[ AnyMsg ] = {
    implicit val s: KafkaExecutorSettings = newSettings( completeProcessStore )

    val fOutstanding: Future[Seq[AnyMsg]] =
      ( if (consume)  KafkaWrapperFlows.srcAll.wireTap( _._2.commitScaladsl() )
      else          KafkaWrapperFlows.srcAll )
        .map( _._1 )
        .completionTimeout(5.seconds)
        .map( Some(_) )
        .recover({ case _: TimeoutException => None })
        .collect({ case Some( msg ) => msg })
        .runFold( Seq(): Seq[AnyMsg] )( _ :+ _ )( s.mat )

    errors.clear()
    Await.result( fOutstanding, Duration.Inf )
  }

  def isAllTidy( isValid: AnyMsg => Boolean = isPiiResult ): Boolean
    = outstanding( true ).forall( isValid )

  def getMsgsOf[T]: ClassTag[T] => Seq[T] = {

    val msgMap: Map[Class[_], Seq[AnyMsg]] =
      outstanding(false)
        .groupBy[Class[_]]( _.getClass )
        .withDefaultValue[Seq[AnyMsg]]( Seq() )

    def msgsOf(ct: ClassTag[T]): Seq[T]
    = msgMap(ct.runtimeClass).asInstanceOf[Seq[T]]

    msgsOf
  }

}
