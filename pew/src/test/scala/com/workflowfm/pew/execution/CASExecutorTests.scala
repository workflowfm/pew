package com.workflowfm.pew.execution

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

import RexampleTypes._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CASExecutorTests extends FlatSpec with Matchers with ProcessExecutorTester {
//implicit val system: ActorSystem = ActorSystem("CASExecutorTests")
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")

  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai, pbi, pci)

  "CASExecutor" should "execute atomic PbI once" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(pbi, Seq(2))

    val r1 = await(f1)
    //r1 should not be empty
  }

  "CASExecutor" should "execute atomic PbI twice concurrently" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(pbi, Seq(2))
    val f2 = ex.execute(pbi, Seq(1))

    val r1 = await(f1)
    //r1 should not be empty
    val r2 = await(f2)
    //r2 should not be empty
  }

  "CASExecutor" should "execute Rexample once" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(ri, Seq(21))

    val r1 = await(f1)
    r1 should be(("PbISleptFor2s", "PcISleptFor1s"))
  }

  "CASExecutor" should "execute Rexample twice concurrently" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(ri, Seq(31))
    val f2 = ex.execute(ri, Seq(12))

    val r1 = await(f1)
    r1 should be(("PbISleptFor3s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor2s"))
  }

  "CASExecutor" should "execute Rexample twice with same timings concurrently" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  "CASExecutor" should "execute Rexample thrice concurrently" in {
    val ex = new CASExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))
    val f3 = ex.execute(ri, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r3 = await(f3)
    r3 should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

}
