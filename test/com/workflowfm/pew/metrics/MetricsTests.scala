package com.workflowfm.pew.metrics

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
import com.workflowfm.pew.stream._
import com.workflowfm.pew.execution._
import RexampleTypes._

@RunWith(classOf[JUnitRunner])
class MetricsTests extends FlatSpec with Matchers with BeforeAndAfterAll with ProcessExecutorTester {
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

  it should "measure things" in {
    val handler = new MetricsHandler[Int]("metrics")
    
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val k1 = ex.subscribe(handler)
    
    val f1 = ex.execute(ri,Seq(11))
    
    await(f1)
    k1.map(_.stop)
    
    handler.keys.size shouldBe 1
    handler.processMetrics.size shouldBe 3
    handler.workflowMetrics.size shouldBe 1
    handler.processMetricsOf(0).size shouldBe 3
  }
  
  it should "output a D3 timeline of 3 Rexample workflows" in {
    val handler = new MetricsHandler[Int]("metrics")
    
    val ex = new AkkaExecutor(pai,pbi,pci,ri)
    val k1 = ex.subscribe(handler)
    
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r3 = await(f3)
    r3 should be (("PbISleptFor1s","PcISleptFor1s"))
	
    k1.map(_.stop)
    
    new MetricsPrinter[Int]()(handler)
    new MetricsD3Timeline[Int]("resources/d3-timeline","Rexample3")(handler)
  }
  
}

