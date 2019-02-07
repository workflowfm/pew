package com.workflowfm.pew.execution

import com.workflowfm.pew._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


/**
  * Shortcut methods for unit testing
  */
trait ProcessExecutorTester {

  def exe(e:ProcessExecutor[_],p:PiProcess,args:Any*)
    = await(e.execute(p,args:Seq[Any]))

  def await[A](f: Future[A], timeout: Duration = 15.seconds): A = try {
    Await.result(f, timeout)
  } catch {
    case e:Throwable => {
      System.out.println("=== RESULT FAILED! ===: " + e.getLocalizedMessage)
      throw e
    }
  }


  //  def awaitf[A](f:Future[Future[A]]):A = try {
  //    Await.result(Await.result(f,15.seconds),15.seconds)
  //  } catch {
  //    case e:Throwable => {
  //      System.out.println("=== RESULT FAILED! ===")
  //      throw e
  //    }
  //  }

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
    
    def apply(x:X)(implicit executor:ProcessExecutor[_]): Future[(Y,Z)] = {
	  implicit val context:ExecutionContext = executor.executionContext

	  executor.execute(this,Seq(x)).map(_.asInstanceOf[(Y,Z)])
    }
  }

  class BadR(pa:Pa,pb:Pb,pc:Pc) extends CompositeProcess { // (X) => (Y,Z)
    override val name = "R"
    override val output = (PiPair(Chan("R_l_a_Y"),Chan("R_r_a_Z")),"z13")
    override val inputs = Seq((Chan("R__a_X"),"cPa_X_1"))
    override val channels = Seq("cPa_X_1","z13")
    
    override val dependencies = Seq(pa,pc)
    
    override val body = PiCut("z16","z15","z5",ParInI("z15","buf13","cPc_B_1",ParOut("z13","b13","oPc_Z_",PiId("buf13","b13","m14"),PiCall<("Pc","cPc_B_1","oPc_Z_"))),PiCut("z8","z7","oPa_lB_A_x_B_rB_",ParInI("z7","cPb_A_1","buf5",ParOut("z5","oPb_Y_","b5",PiCall<("Pb","cPb_A_1","oPb_Y_"),PiId("buf5","b5","m6"))),PiCall<("Pa","cPa_X_1","oPa_lB_A_x_B_rB_")))
    
    def apply(x:X)(implicit executor:ProcessExecutor[_]): Future[Option[(Y,Z)]] = {
  	  implicit val context:ExecutionContext = executor.executionContext 

  	  executor.execute(this,Seq(x)).map(_.asInstanceOf[Option[(Y,Z)]])
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
	  Thread.sleep(arg0 * 100)
	  iname + "SleptFor" + arg0 +"s"
    }
  }
  class PcI(s:String="PcI") extends Pc {
    override def iname = s
    override def apply( arg0 :B ) :Z = {
	  System.out.println(iname + " sleeping for: " + arg0 + "s")
	  Thread.sleep(arg0 * 100)
	  iname + "SleptFor" + arg0 +"s"
    }
  }

  class PcIF(s:String="PcI") extends Pc {
    override def iname = s
    override def apply( arg0 :B ) :Z = {
	  System.out.println(iname + " sleeping for: " + arg0 + "s")
	  throw new Exception("Fail")
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

}
