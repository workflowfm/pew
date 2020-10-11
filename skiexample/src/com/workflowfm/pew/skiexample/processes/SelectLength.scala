package com.workflowfm.pew.skiexample.processes

import scala.concurrent._

import com.workflowfm.pew._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

trait SelectLength extends ((HeightCM, WeightKG) => Future[LengthCM]) with AtomicProcess {
  override val name = "SelectLength"

  override val output: (Chan, String) =
    (Chan("SelectLength__a_LengthCM"), "oSelectLength_LengthCM_")

  override val inputs: Seq[(Chan, String)] = Seq(
    (Chan("SelectLength__a_HeightCM"), "cSelectLength_HeightCM_1"),
    (Chan("SelectLength__a_WeightKG"), "cSelectLength_WeightKG_2")
  )

  override val channels: Seq[String] =
    Seq("cSelectLength_HeightCM_1", "cSelectLength_WeightKG_2", "oSelectLength_LengthCM_")

  def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
    case Seq(o1, o2) =>
      this(PiObject.getAs[HeightCM](o1), PiObject.getAs[WeightKG](o2)) map PiObject.apply
  }
}
