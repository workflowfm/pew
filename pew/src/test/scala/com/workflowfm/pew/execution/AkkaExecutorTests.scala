package com.workflowfm.pew.execution

import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration._

import RexampleTypes._
import akka.actor.ActorSystem
import org.junit.runner.RunWith
import org.scalatest.{ FlatSpec, Matchers, BeforeAndAfterAll }
import org.scalatest.junit.JUnitRunner

import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.stream._

@RunWith(classOf[JUnitRunner])
class AkkaExecutorTests
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("AkkaExecutorTests")
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")
  implicit val timeout: FiniteDuration = 10.seconds

  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val pci2 = new PcI("PcX")
  val pcif = new PcIF
  val ri = new R(pai, pbi, pci)
  val ri2 = new R(pai, pbi, pci2)
  val rif = new R(pai, pbi, pcif)

  override def afterAll: Unit = {
    Await.result(system.terminate(), 10.seconds)
  }

  it should "execute atomic PbI once" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(pbi, Seq(1))

    await(f1).success.value should be("PbISleptFor1s")
  }

  it should "execute atomic PbI twice concurrently" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(pbi, Seq(2))
    val f2 = ex.execute(pbi, Seq(1))

    await(f1).success.value should be("PbISleptFor2s")
    await(f2).success.value should be("PbISleptFor1s")
  }

  it should "execute Rexample once" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(21))

    await(f1).success.value should be(("PbISleptFor2s", "PcISleptFor1s"))
  }

  it should "execute Rexample once with same timings" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample twice concurrently" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(31))
    val f2 = ex.execute(ri, Seq(12))

    await(f1).success.value should be(("PbISleptFor3s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor2s"))
  }

  it should "execute Rexample twice with same timings concurrently" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample thrice concurrently" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))
    val f3 = ex.execute(ri, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f3).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample twice, each with a different component" in {
    val ex = new AkkaExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri2, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcXSleptFor1s"))
  }

  it should "handle a failing atomic process" in {
    val p = new FailP
    val ex = new AkkaExecutor()
    val f1 = ex.execute(p, Seq(1))

    val e = await(f1).failure.exception
    e shouldBe a[RemoteProcessException[_]]
    e.getMessage should be("FailP")
  }

  it should "handle a failing composite process" in {
    val ex = new AkkaExecutor()
    val f1 = rif(21)(ex) //ex.execute(rif,Seq(21))

    val e = await(f1).failure.exception
    e shouldBe a[RemoteProcessException[_]]
    e.getMessage shouldBe ("Fail")
  }

  it should "execute Rexample with a CounterHandler" in {
    val ex = new AkkaExecutor()
    val factory = new CounterHandlerFactory[UUID]
    val kill = ex.subscribe(new PrintEventHandler)

    val f1 = ex.call(ri, Seq(21), factory) flatMap (_.future)

    await(f1).success.value should be(11)
    kill.map(_.stop)
  }

  it should "allow separate handlers per executor" in {
    val ex = new AkkaExecutor()
    val ex2 = new AkkaExecutor()
    val factory = new CounterHandlerFactory[UUID]
    val k1 = ex.subscribe(new PrintEventHandler)
    val k2 = ex2.subscribe(new PrintEventHandler)

    val f1 = ex.call(ri, Seq(99), factory) flatMap (_.future)
    val f2 = ex2.execute(ri, Seq(11))

    //    val r2 = await(f2)
    //    r2 should be (("PbISleptFor2s","PcISleptFor1s"))

    await(f1).success.value should be(11)

    k1.map(_.stop)
    k2.map(_.stop)
  }

  it should "allow separate handlers for separate workflows" in {
    val ex = new AkkaExecutor()
    val factory = new CounterHandlerFactory[UUID]

    val f1 = ex.call(ri, Seq(55), factory) flatMap (_.future)
    val f2 = ex.call(ri, Seq(11), factory) flatMap (_.future)

    //    val r2 = await(f2)
    //    r2 should be (("PbISleptFor2s","PcISleptFor1s"))

    await(f1).success.value should be(11)
    await(f2).success.value should be(11)

  }

  it should "unsubscribe handlers successfully" in {
    val ex = new AkkaExecutor()
    val factory = new CounterHandlerFactory[UUID]
    val kill = ex.subscribe(new PrintEventHandler)
    kill.map(_.stop)

    val f1 = ex.call(ri, Seq(55), factory) flatMap (_.future)
    val f2 = ex.call(ri, Seq(11), factory) flatMap (_.future)

    //    val r2 = await(f2)
    //    r2 should be (("PbISleptFor2s","PcISleptFor1s"))

    await(f1).success.value should be(11)
    await(f2).success.value should be(11)
  }

  it should "execute a reduced Rexample instance" in {
    val ex = new AkkaExecutor()

    val instance = PiInstance(0, ri, PiObject(11)).reduce.postResult(0, PiObject((1, 2))).reduce
    val f1 = ex.execute(instance)

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor2s"))
  }
}
