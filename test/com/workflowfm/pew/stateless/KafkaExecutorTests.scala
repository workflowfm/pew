package com.workflowfm.pew.stateless

import akka.Done
import akka.actor.ActorSystem
import com.workflowfm.pew.execution.RexampleTypes._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.components.{KafkaConnectors, KafkaWrapperFlows}
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.{CompleteKafkaExecutor, MinimalKafkaExecutor}
import com.workflowfm.pew.{PiInstance, PiProcessStore, SimpleProcessStore}
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class KafkaExecutorTests extends FlatSpec with Matchers with BeforeAndAfterAll with ProcessExecutorTester {

  class PcIWait( s: String = "PcI" ) extends Pc {
    override def iname: String = s

    private var promise: Promise[Done] = Promise[Done]()

    override def apply( arg0 :B ) :Z = {
      Await.result( promise.future, Duration.Inf )
      iname + "SleptFor" + arg0 +"s"
    }

    def release(): Unit = {
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

  val newSettings: PiProcessStore => KafkaExecutorSettings
    = new KafkaExecutorSettings( _, system, executionContext )

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

  def makeExecutor(store: SimpleProcessStore): MinimalKafkaExecutor[(Y, Z)] = {
    implicit val s: KafkaExecutorSettings = newSettings( store )

    CompleteKafkaExecutor[(Y, Z)]
    // SeqRedKafkaExecutor[(Y, Z)]
  }

  val isPiiResult: AnyMsg => Boolean = _.isInstanceOf[PiiResult[_]]

  // TODO: Fix consumer shutdown: https://github.com/akka/alpakka-kafka/issues/166
  def outstanding( consume: Boolean ): Seq[ AnyMsg ] = {
    implicit val s: KafkaExecutorSettings = newSettings( completeProcessStore )

    val fOutstanding: Future[Seq[AnyMsg]]
      = KafkaWrapperFlows.srcAll
      .map({ case (msg, offset) => offset.commitScaladsl(); msg })
      .completionTimeout(5.seconds)
      .map( Some(_) )
      .recover({ case _: TimeoutException => None })
      .collect({ case Some( msg ) => msg })
      .runFold( Seq(): Seq[AnyMsg] )( _ :+ _ )( s.materializer )

    Await.result( fOutstanding, Duration.Inf )
  }

  def isAllTidy( isValid: AnyMsg => Boolean = isPiiResult ): Boolean
    = outstanding( true ).forall( isValid )

  // Ensure there are no outstanding messages before starting testing.
  isAllTidy()

  it should "execute atomic PbI once" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute( pbi, Seq(1) )

    await( f1 ) should be ("PbISleptFor1s")
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute atomic PbI twice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))

    await(f1) should be ("PbISleptFor2s")
    await(f2) should be ("PbISleptFor1s")
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample once" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample once with same timings" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample twice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))

    await(f1) should be (("PbISleptFor3s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor2s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample twice with same timings concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample thrice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f3) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "execute Rexample twice, each with a differnt component" in {

    val ex = makeExecutor( completeProcessStore )
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri2,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcXSleptFor1s"))
    ex.syncShutdown()

    isAllTidy() should be (true)
  }

  it should "handle a failing atomic process" in {

    val ex = makeExecutor( failureProcessStore )
    val f1 = ex.execute( failp, Seq(1) )

    try await(f1)
    catch {
     case e: Exception =>
       e.getMessage should be ("FailP")
    }

    ex.syncShutdown()
    isAllTidy() should be (true)
  }

  it should "handle a failing composite process" in {

    val ex = makeExecutor( failureProcessStore )
    val f1 = ex.execute( rif, Seq(21) )

    try await(f1)
    catch {
      case e: Exception =>
        e.getMessage should be ("Fail")
    }

    ex.syncShutdown()
    isAllTidy() should be (true)
   }

  it should "2 separate executors should execute with same timings concurrently" in {

    val ex1 = makeExecutor(completeProcessStore)
    val ex2 = makeExecutor(completeProcessStore)

    val f1 = ex1.execute(ri,Seq(11))
    val f2 = ex2.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))

    ex1.syncShutdown()
    ex2.syncShutdown()

    isAllTidy() should be (true)
   }

  it should "an executor should restart successfully" in {

    val ourPiiId: ObjectId = new ObjectId

    {
      val ex = makeExecutor( shutdownProcessStore )
      ex.executeWith( ourPiiId, ri, Seq(21) )

      Thread.sleep( 5.seconds.toMillis )
      ex.syncShutdown()
    }

    {
      val outstandingMsgs: Map[String, Int]
        = outstanding(false)
          .filter(piiId(_) == ourPiiId)
          .map(_.getClass.getSimpleName)
          .groupBy(identity)
          .mapValues(_.size)
          .withDefaultValue(0)

      val expectedMsgs: Map[String, Int]
        = Map(
          "ReduceRequest" -> 0,
          "SequenceRequest" -> 0,
          "Assignment" -> 1,
          "PiiUpdate" -> 1
        )

      outstandingMsgs should be (expectedMsgs)
    }

    {
      val ex = makeExecutor( completeProcessStore )
      val f2: Future[(Y,Z)] = ex.connect( ourPiiId, ri, Seq(21) )._2
      pciw.release()

      await(f2) should be (("PbISleptFor2s","PcISleptFor1s"))
    }

    isAllTidy() should be (true)
  }

  it should "execute correctly with an outstanding PiiUpdate" in {

    // Construct and send an outstanding PiiUpdate message to test robustness.
    val oldMsg: PiiUpdate = PiiUpdate( PiInstance.forCall( ObjectId.get, ri, 2, 1 ) )
    KafkaConnectors.sendSingleMessage( oldMsg )( newSettings( completeProcessStore ) )

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri, Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    // We don't care what state we leave the outstanding message in, provided we clean our own state.
    def isValid( msg: AnyMsg ): Boolean = isPiiResult( msg ) || piiId( msg ) == oldMsg.pii.id

    isAllTidy( isValid ) should be (true)
  }

}
