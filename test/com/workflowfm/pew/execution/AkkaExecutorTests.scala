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
  val pci2 = new PcI("PcX")
  val pcif = new PcIF
  val ri = new R(pai,pbi,pci)
  val ri2 = new R(pai,pbi,pci2)
  val rif = new R(pai,pbi,pcif)

  override def afterAll:Unit = {
    Await.result(system.terminate(),10.seconds)
  }
  
  it should "execute atomic PbI once" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(1))
   
    val r1 = await(f1)
    r1 should be ("PbISleptFor1s")
	}

  it should "execute atomic PbI twice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))
   
    val r1 = await(f1)
    r1 should be ("PbISleptFor2s")
    val r2 = await(f2)
    r2 should be ("PbISleptFor1s")
	}
  
  it should "execute Rexample once" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(21))
   
    val r1 = await(f1)
    r1 should be (("PbISleptFor2s","PcISleptFor1s"))
	}

  it should "execute Rexample once with same timings" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
   
    val r1 = await(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
	}
 
  it should "execute Rexample twice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    ex.subscribe(new PrintEventHandler("printer"))
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))
    
    val r1 = await(f1)
    r1 should be (("PbISleptFor3s","PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor2s"))
    ex.unsubscribe("printer")
	}

  it should "execute Rexample twice with same timings concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor1s"))
	}
  
  it should "execute Rexample thrice concurrently" in {
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r3 = await(f3)
    r3 should be (("PbISleptFor1s","PcISleptFor1s"))
	}
  
  it should "execute Rexample twice, each with a differnt component" in {
    val ex = new AkkaExecutor(pai,pbi,pci,pci2,ri,ri2)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri2,Seq(11))
    
    val r1 = await(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be (("PbISleptFor1s","PcXSleptFor1s"))
	}
  
  it should "handle a failing atomic process" in {
    val p = new FailP
    val ex = new AkkaExecutor(p)
    val f1 = ex.execute(p,Seq(1))
    
    try {
      await(f1)
    } catch {
      case (e:Exception) => e.getMessage.contains("Exception: FailP") should be (true)
    }
	}
  
  it should "handle a failing composite process" in {
    val ex = new AkkaExecutor(pai,pbi,pcif,rif)
    val f1 = rif(21)(ex)//ex.execute(rif,Seq(21)) 
   
    try {
      await(f1)
    } catch {
      case (e:Exception) => e.getMessage.contains("Exception: Fail") should be (true)
    }
	}
}

