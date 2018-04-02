package com.workflowfm.pew.execution

import akka.actor.ActorSystem
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent._
import scala.concurrent.Await
import scala.concurrent.duration._
import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import RexampleTypes._

@RunWith(classOf[JUnitRunner])
class AkkaExecutorTests extends FlatSpec with Matchers with BeforeAndAfterAll with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("AkkaExecutorTests")
  implicit val executionContext = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")  
  implicit val timeout:FiniteDuration = 10.seconds
  
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai,pbi,pci)

  override def afterAll:Unit = {
    Await.result(system.terminate(),10.seconds)
  }
  
  it should "execute atomic PbI once" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(1))
   
    val r1 = await(f1)
    r1 should not be empty
    r1 should be (Some("PbSleptFor1s"))
	}

  it should "execute atomic PbI twice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))
   
    val r1 = await(f1)
    r1 should not be empty
    r1 should be (Some("PbSleptFor2s"))
    val r2 = await(f2)
    r2 should not be empty
    r2 should be (Some("PbSleptFor1s"))
	}
  
  it should "execute Rexample once" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(21))
   
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor2s","PcSleptFor1s")))
	}

  it should "execute Rexample once with same timings" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
   
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
	}
 
  it should "execute Rexample twice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor3s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor2s"))) 
	}

  it should "execute Rexample twice with same timings concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
	}
  
  it should "execute Rexample thrice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r3 = await(f3)
    r3 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
	}
}
