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


@RunWith(classOf[JUnitRunner])
class SingleStateExecutorTests extends FlatSpec with Matchers with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("SingleStateExecutorTests")
  implicit val executionContext = system.dispatchers.lookup("akka.my-dispatcher")  
  
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai,pbi,pci)
  
  "SingleStateExecutor" should "execute Rexample concurrently" in {
		exe(new SingleStateExecutor(pai,pbi,pci,ri),ri,13)//.isEmpty should be( false )
		exe(new SingleStateExecutor(pai,pbi,pci,ri),ri,31)//.isEmpty should be( false )
	}
	
  
  
	"SingleStateExecutor" should "execute C1" in {
		exe(new SingleStateExecutor(P1,C1),C1,("OH","HAI!")) should be( Some("OH++HAI!") )
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
		exe(new SingleStateExecutor(P2A,P2B,C2),C2,"HI:") should be( Some("HI:AABB") )
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
		exe(new SingleStateExecutor(P3A,P3B,C3),C3,"HI:") should be( Some(("HI:AARR","HI:BB")) )
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


package object RexampleTypes
{
	// TODO: Instantiate the following types:
	type A = Int
	type B = Int
	type X = Int
	type Y = String
	type Z = String
	
	trait Pa extends ((X) => (A,B)) with AtomicProcess {
		override val name = "Pa"
		override val output = (PiPair(Chan("Pa_l_a_A"),Chan("Pa_r_a_B")),"oPa_lB_A_x_B_rB_")
		override val inputs = Seq((Chan("Pa__a_X"),"cPa_X_1"))
		override val channels = Seq("cPa_X_1","oPa_lB_A_x_B_rB_")

		def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
		  case Seq(o1) => Future { PiObject(this(PiObject.getAs[X](o1))) }
		}
	}

	trait Pb extends ((A) => Y) with AtomicProcess {
	  override val name = "Pb"
		override val output = (Chan("Pb__a_Y"),"oPb_Y_")
		override val inputs = Seq((Chan("Pb__a_A"),"cPb_A_1"))
		override val channels = Seq("cPb_A_1","oPb_Y_")

		def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
				case Seq(o1) => Future { PiObject(this(PiObject.getAs[A](o1))) }
		}
	}

	trait Pc extends ((B) => Z) with AtomicProcess {
	  override val name = "Pc"
		override val output = (Chan("Pc__a_Z"),"oPc_Z_")
		override val inputs = Seq((Chan("Pc__a_B"),"cPc_B_1"))
		override val channels = Seq("cPc_B_1","oPc_Z_")

		def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
				case Seq(o1) => Future { PiObject(this(PiObject.getAs[B](o1))) }
		}
	}
	
	class R(pa:Pa,pb:Pb,pc:Pc) extends CompositeProcess { // (X) => (Y,Z)
  	override val name = "R"
  	override val output = (PiPair(Chan("R_l_a_Y"),Chan("R_r_a_Z")),"z13")
  	override val inputs = Seq((Chan("R__a_X"),"cPa_X_1"))
  	override val channels = Seq("cPa_X_1","z13")
  
  	override val dependencies = Seq(pa,pb,pc)
  
  	override val body = PiCut("z16","z15","z5",ParInI("z15","buf13","cPc_B_1",ParOut("z13","b13","oPc_Z_",PiId("buf13","b13","m14"),PiCall<("Pc","cPc_B_1","oPc_Z_"))),PiCut("z8","z7","oPa_lB_A_x_B_rB_",ParInI("z7","cPb_A_1","buf5",ParOut("z5","oPb_Y_","b5",PiCall<("Pb","cPb_A_1","oPb_Y_"),PiId("buf5","b5","m6"))),PiCall<("Pa","cPa_X_1","oPa_lB_A_x_B_rB_")))
  	
  	def apply(x:X)(implicit executor:FutureExecutor): Option[(Y,Z)] =
  		executor.execute(this,Seq(x)).asInstanceOf[Option[(Y,Z)]]
}
	class BadR(pa:Pa,pb:Pb,pc:Pc) extends CompositeProcess { // (X) => (Y,Z)
  	override val name = "R"
  	override val output = (PiPair(Chan("R_l_a_Y"),Chan("R_r_a_Z")),"z13")
  	override val inputs = Seq((Chan("R__a_X"),"cPa_X_1"))
  	override val channels = Seq("cPa_X_1","z13")
  
  	override val dependencies = Seq(pa,pc)
  
  	override val body = PiCut("z16","z15","z5",ParInI("z15","buf13","cPc_B_1",ParOut("z13","b13","oPc_Z_",PiId("buf13","b13","m14"),PiCall<("Pc","cPc_B_1","oPc_Z_"))),PiCut("z8","z7","oPa_lB_A_x_B_rB_",ParInI("z7","cPb_A_1","buf5",ParOut("z5","oPb_Y_","b5",PiCall<("Pb","cPb_A_1","oPb_Y_"),PiId("buf5","b5","m6"))),PiCall<("Pa","cPa_X_1","oPa_lB_A_x_B_rB_")))
  	
  	def apply(x:X)(implicit executor:FutureExecutor): Option[(Y,Z)] =
  		executor.execute(this,Seq(x)).asInstanceOf[Option[(Y,Z)]]
}
}
class PaI extends Pa {
  override def iname = "PaI"
	override def apply( arg0 :X ) :(A,B) = { 
	  (arg0 / 10,arg0 % 10)
	}
}
class PbI extends Pb {
  override def iname = "PbI"
	override def apply( arg0 :A ) :Y = {
		System.out.println(iname + " sleeping for: " + arg0 + "s")
		Thread.sleep(arg0 * 1000)
		iname + "SleptFor" + arg0 +"s"
	}
}
class PcI(s:String="PcI") extends Pc {
  override def iname = s 
	override def apply( arg0 :B ) :Z = {
		System.out.println(iname + " sleeping for: " + arg0 + "s")
		Thread.sleep(arg0 * 1000)
		iname + "SleptFor" + arg0 +"s"
	}
}

class FailP extends AtomicProcess {
  override val name = "FailP"
	override val output = (Chan("Pc__a_Z"),"oPc_Z_")
	override val inputs = Seq((Chan("Pc__a_B"),"cPc_B_1"))
	override val channels = Seq("cPc_B_1","oPc_Z_")

	def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
			case Seq(o1) => Future.failed(new Exception("FailP"))
	}
}
