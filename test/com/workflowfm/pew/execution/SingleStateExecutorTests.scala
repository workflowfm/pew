package com.workflowfm.pew.execution

import RexampleTypes._
import akka.actor.ActorSystem
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.workflowfm.pew._
import com.workflowfm.pew.stream._


@RunWith(classOf[JUnitRunner])
class SingleStateExecutorTests extends FlatSpec with Matchers with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("SingleStateExecutorTests")
  implicit val executionContext = ExecutionContext.global//system.dispatchers.lookup("akka.my-dispatcher")  
  
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai,pbi,pci)
  val pcif = new PcIF
  val rif = new R(pai,pbi,pcif)
  
  "SingleStateExecutor" should "execute Rexample concurrently" in {
    val executor = new SingleStateExecutor(pai,pbi,pci,ri)
    executor.subscribe(new PrintEventHandler("printer"))
		exe(executor,ri,13)//.isEmpty should be( false )
		//exe(new SingleStateExecutor(pai,pbi,pci,ri),ri,31)//.isEmpty should be( false )
	}
	
  "SingleStateExecutor" should "handle a failing component process" in {
    val ex = new SingleStateExecutor(pai,pbi,pcif,rif)
    val f1 = rif(21)(ex)//ex.execute(rif,Seq(21)) 
   
    try {
      await(f1)
    } catch {
      case (e:Exception) => e.getMessage.contains("Exception: Fail") should be (true)
    }
	}
  
//  it should "fail properly when a component process doesn't exist" in {
//    val ex = new SingleStateExecutor(rbad)
//    ex.subscribe(new PrintEventHandler("printer"))
//    val f1 = rif(21)(ex)
//    
//    a [ProcessExecutor.NoSuchInstanceException] should be thrownBy await(f1)
//	}
  
	"SingleStateExecutor" should "execute C1" in {
	  val executor = new SingleStateExecutor(P1,C1)
		exe(executor,C1,("OH","HAI!")) should be( "OH++HAI!" )
	}
	
	object P1 extends AtomicProcess { // X,Y -> (X++Y)
    override val name = "P1"
    override val output = (Chan("ZA"),"Z")
    override val inputs = Seq((Chan("XA"),"X"),(Chan("YA"),"Y"))
    override val channels = Seq("X","Y","Z")
   
    def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "++" + PiObject.getAs[String](args.tail.headOption.get)))
	}
	
	object C1 extends CompositeProcess {
    override val name = "C1"
    override val output = (Chan("ZA"),"Z")
    override val inputs = Seq((PiPair(Chan("XCA"),Chan("XCB")),"XC")) 
    override val body = ParInI("XC","LC","RC",PiCall<("P1","LC","RC","Z"))
    override val dependencies = Seq(P1)
	}
	
	
	"SingleStateExecutor" should "execute C2" in {
		exe(new SingleStateExecutor(P2A,P2B,C2),C2,"HI:") should be( "HI:AABB" )
	}
	
	object P2A extends AtomicProcess { // X -> XAA
    override val name = "P2A"
    override val output = (Chan("P2A-Z"),"ZA")
    override val inputs = Seq((Chan("P2A-X"),"XA"))
    override val channels = Seq("XA","ZA")
   
    def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "AA"))
	}

	object P2B extends AtomicProcess { // X -> XB
    override val name = "P2B"
    override val output = (Chan("P2B-Z"),"ZB")
    override val inputs = Seq((Chan("P2B-X"),"XB"))
    override val channels = Seq("XB","ZB")
   
    def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "BB"))
	}
	
	object C2 extends CompositeProcess {
    override val name = "C2"
    override val output = (Chan("C2-A"),"ZB")
    override val inputs = Seq((Chan("C2-X"),"XA")) 
    override val body = PiCut("z2","ZA","XB",PiCall<("P2A","XA","ZA"),PiCall<("P2B","XB","ZB"))
    override val dependencies = Seq(P2A,P2B)
	}
	
	
  "SingleStateExecutor" should "execute C3" in {
		exe(new SingleStateExecutor(P3A,P3B,C3),C3,"HI:") should be( ("HI:AARR","HI:BB") )
	}
	
	object P3A extends AtomicProcess { // X -> (XAA,XBB)
    override val name = "P3A"
    override val output = (PiPair(Chan("P3A-ZA"),Chan("P3A-ZB")),"ZA")
    override val inputs = Seq((Chan("P3A-X"),"XA"))
    override val channels = Seq("XA","ZA")
   
    def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] =
      Future.successful(PiObject((PiObject.getAs[String](args.headOption.get) + "AA",PiObject.getAs[String](args.headOption.get) + "BB")))
	}
  
	object P3B extends AtomicProcess { // X -> XRR
    override val name = "P3B"
    override val output = (Chan("P3B-Z"),"ZB")
    override val inputs = Seq((Chan("P3B-X"),"XB"))
    override val channels = Seq("XB","ZB")
   
    def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "RR"))
	}
		
	object C3 extends CompositeProcess {
    override val name = "C3"
    override val output = (PiPair(Chan("ZCA"),Chan("ZCB")),"ZC")
    override val inputs = Seq((Chan("C2-X"),"XA")) 
    
    val x = ParInI("z7","XB","II",ParOut("ZC","ZB","IO",PiCall<("P3B","XB","ZB"),PiId("II","IO","IX")))
    
    override val body = PiCut("z8","ZA","z7",PiCall<("P3A","XA","ZA"),x)
    override val dependencies = Seq(P3A,P3B)
	}
}

