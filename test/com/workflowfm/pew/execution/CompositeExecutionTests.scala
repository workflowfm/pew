package com.workflowfm.pew.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.workflowfm.pew._

@RunWith(classOf[JUnitRunner])
class CompositeExecutionTests extends FlatSpec with Matchers {
  implicit val context: ExecutionContext = ExecutionContext.global

  "SingleBlockingExecutor" should "execute C1" in {
    SingleBlockingExecutor() call (C1, Seq(PiPair(PiItem("OH"), PiItem("HAI!")))) should be(
      Some("OH++HAI!")
    )
  }

  object P1 extends AtomicProcess { // X,Y -> (X++Y)
    override val name = "P1"
    override val output = (Chan("ZA"), "Z")
    override val inputs = Seq((Chan("XA"), "X"), (Chan("YA"), "Y"))
    override val channels = Seq("X", "Y", "Z")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(
        PiObject(
          PiObject.getAs[String](args.headOption.get) + "++" + PiObject.getAs[String](
                args.tail.headOption.get
              )
        )
      )
  }

  object C1 extends CompositeProcess {
    override val name = "C1"
    override val output = (Chan("ZA"), "Z")
    override val inputs = Seq((PiPair(Chan("XCA"), Chan("XCB")), "XC"))
    override val body = ParInI("XC", "LC", "RC", PiCall < ("P1", "LC", "RC", "Z"))
    override val dependencies = Seq(P1)
  }

  "SingleBlockingExecutor" should "execute C2" in {
    SingleBlockingExecutor() call (C2, Seq(PiItem("HI:"))) should be(Some("HI:AABB"))
  }

  object P2A extends AtomicProcess { // X -> XAA
    override val name = "P2A"
    override val output = (Chan("P2A-Z"), "ZA")
    override val inputs = Seq((Chan("P2A-X"), "XA"))
    override val channels = Seq("XA", "ZA")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "AA"))
  }

  object P2B extends AtomicProcess { // X -> XB
    override val name = "P2B"
    override val output = (Chan("P2B-Z"), "ZB")
    override val inputs = Seq((Chan("P2B-X"), "XB"))
    override val channels = Seq("XB", "ZB")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "BB"))
  }

  object C2 extends CompositeProcess {
    override val name = "C2"
    override val output = (Chan("C2-A"), "ZB")
    override val inputs = Seq((Chan("C2-X"), "XA"))

    override val body =
      PiCut("z2", "ZA", "XB", PiCall < ("P2A", "XA", "ZA"), PiCall < ("P2B", "XB", "ZB"))
    override val dependencies = Seq(P2A, P2B)
  }

  "SingleBlockingExecutor" should "execute C3" in {
    SingleBlockingExecutor() call (C3, Seq(PiItem("HI:"))) should be(Some(("HI:AARR", "HI:BB")))
  }

  object P3A extends AtomicProcess { // X -> (XAA,XBB)
    override val name = "P3A"
    override val output = (PiPair(Chan("P3A-ZA"), Chan("P3A-ZB")), "ZA")
    override val inputs = Seq((Chan("P3A-X"), "XA"))
    override val channels = Seq("XA", "ZA")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(
        PiObject(
          (
            PiObject.getAs[String](args.headOption.get) + "AA",
            PiObject.getAs[String](args.headOption.get) + "BB"
          )
        )
      )
  }

  object P3B extends AtomicProcess { // X -> XRR
    override val name = "P3B"
    override val output = (Chan("P3B-Z"), "ZB")
    override val inputs = Seq((Chan("P3B-X"), "XB"))
    override val channels = Seq("XB", "ZB")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "RR"))
  }

  object C3 extends CompositeProcess {
    override val name = "C3"
    override val output = (PiPair(Chan("ZCA"), Chan("ZCB")), "ZC")
    override val inputs = Seq((Chan("C2-X"), "XA"))

    val x = ParInI(
      "z7",
      "XB",
      "II",
      ParOut("ZC", "ZB", "IO", PiCall < ("P3B", "XB", "ZB"), PiId("II", "IO", "IX"))
    )

    override val body = PiCut("z8", "ZA", "z7", PiCall < ("P3A", "XA", "ZA"), x)
    override val dependencies = Seq(P3A, P3B)
  }
}
