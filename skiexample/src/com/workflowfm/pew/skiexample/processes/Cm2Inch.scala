package com.workflowfm.pew.skiexample.processes

import scala.concurrent._

import com.workflowfm.pew._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

trait CM2Inch extends ((LengthCM) => Future[LengthInch]) with AtomicProcess {
  override val name = "CM2Inch"
  override val output: (Chan, String) = (Chan("CM2Inch__a_LengthInch"), "oCM2Inch_LengthInch_")

  override val inputs: Seq[(Chan, String)] = Seq(
    (Chan("CM2Inch__a_LengthCM"), "cCM2Inch_LengthCM_1")
  )
  override val channels: Seq[String] = Seq("cCM2Inch_LengthCM_1", "oCM2Inch_LengthInch_")

  def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
    case Seq(o1) => this(PiObject.getAs[LengthCM](o1)) map PiObject.apply
  }
}
