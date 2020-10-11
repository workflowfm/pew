package com.workflowfm.pew.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent._
import ExecutionContext.Implicits.global
import com.workflowfm.pew._

@RunWith(classOf[JUnitRunner])
class AtomicExecutionTests extends FlatSpec with Matchers {

  object P1 extends AtomicProcess { // X -> X++
    override val name = "P1"
    override val output = (Chan("ZA"), "Z")
    override val inputs = Seq((Chan("XA"), "X"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[String](args.headOption.get) + "++"))
  }

  "AtomicProcessExecutor" should "execute P1" in {
    AtomicProcessExecutor(P1).call(PiItem("OHHAI!")) should be(Some("OHHAI!++"))
  }

  object P2 extends AtomicProcess { // X,Y -> X++Y
    override val name = "P2"
    override val output = (Chan("ZA"), "Z")
    override val inputs = Seq((Chan("XA"), "X"), (Chan("YA"), "Y"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(
        PiObject(
          PiObject.getAs[String](args.headOption.get) + "++" + PiObject.getAs[String](
                args.tail.headOption.get
              )
        )
      )
  }

  "AtomicProcessExecutor" should "execute P2" in {
    AtomicProcessExecutor(P2).call(PiItem("OH"), PiItem("HAI!")) should be(Some("OH++HAI!"))
  }

  object P3 extends AtomicProcess { // X,Y -> (X++,Y++)
    override val name = "P3"
    override val output = (PiPair(Chan("ZA"), Chan("ZB")), "Z")
    override val inputs = Seq((Chan("XA"), "X"), (Chan("YA"), "Y"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1, o2) =>
        Future.successful(PiObject(this(PiObject.getAs[String](o1), PiObject.getAs[String](o2))))
    }

    def apply(o1: String, o2: String) = (o1 + "++", o2 + "++")
  }

  "AtomicProcessExecutor" should "execute P3" in {
    AtomicProcessExecutor(P3).call(PiItem("OH"), PiItem("HAI!")) should be(Some(("OH++", "HAI!++")))
  }

  object P4 extends AtomicProcess { // (X,Y) -> (X++Y)
    override val name = "P4"
    override val output = (Chan("ZA"), "Z")
    override val inputs = Seq((PiPair(Chan("XA"), Chan("XB")), "X"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
      Future.successful(PiObject(PiObject.getAs[(String, String)](args.headOption.get) match {
        case (a, b) => a + "++" + b
      }))
  }

  "AtomicProcessExecutor" should "execute P4" in {
    AtomicProcessExecutor(P4).call(PiPair(PiItem("OH"), PiItem("HAI!"))) should be(Some("OH++HAI!"))
  }

  object P5 extends AtomicProcess { // (X,Y) (P,Q) -> (X++P,Y++Q)
    override val name = "P5"
    override val output = (PiPair(Chan("ZA"), Chan("ZB")), "Z")

    override val inputs =
      Seq((PiPair(Chan("XA"), Chan("XB")), "X"), (PiPair(Chan("YA"), Chan("YB")), "Y"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = {
      val l = PiObject.getAs[(String, String)](args.headOption.get)
      val r = PiObject.getAs[(String, String)](args.tail.headOption.get)
      (l, r) match {
        case ((x, y), (p, q)) => Future.successful(PiObject((x + "++" + p, y + "++" + q)))
      }
    }
  }

  "AtomicProcessExecutor" should "execute P5" in {
    AtomicProcessExecutor(P5).call(
      PiPair(PiItem("OH"), PiItem("0H")),
      PiPair(PiItem("HAI!"), PiItem("H41!"))
    ) should be(Some(("OH++HAI!", "0H++H41!")))
  }

  object P6 extends AtomicProcess { // X -> if X>5 then X - 1 else X + 1
    override val name = "P6"
    override val output = (PiOpt(Chan("ZA"), Chan("ZB")), "Z")
    override val inputs = Seq((Chan("XA"), "X"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = {
      val x = PiObject.getAs[Int](args.headOption.get)
      if (x > 5) Future.successful(PiLeft(PiItem(x - 1)))
      else Future.successful(PiRight(PiItem(x + 1)))
    }
  }

  "AtomicProcessExecutor" should "execute P6" in {
    AtomicProcessExecutor(P6).call(PiItem(6)) should be(Some(Left(5)))
    AtomicProcessExecutor(P6).call(PiItem(0)) should be(Some(Right(1)))
  }

  object P7 extends AtomicProcess { // (int or string) -> (int +1 or string++)
    override val name = "P7"
    override val output = (PiOpt(Chan("ZA"), Chan("ZB")), "Z")
    override val inputs = Seq((PiOpt(Chan("XA"), Chan("XB")), "X"))

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = {
      PiObject.getAs[Either[Int, String]](args.headOption.get) match {
        case Left(i) => Future.successful(PiObject(Left(i + 1)))
        case Right(s) => Future.successful(PiObject(Right(s + "++")))
      }
    }
  }

  "AtomicProcessExecutor" should "execute P7" in {
    AtomicProcessExecutor(P7).call(PiLeft(PiItem(6))) should be(Some(Left(7)))
    AtomicProcessExecutor(P7).call(PiRight(PiItem("HI!"))) should be(Some(Right("HI!++")))
  }
}
