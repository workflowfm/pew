package com.workflowfm.pew

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AtomicCallTests extends FlatSpec with Matchers with PiStateTester {

  val proc1: DummyProcess = DummyProcess("PROC", "R", Seq((Chan("INPUT"), "C")))

  it should "eliminate a call to a non-existing process" in {
    reduceOnce(In("A", "C", PiCall < ("PROC", "R", "C")), Out("A", Chan("X"))) should be(
      Some(PiState())
    ) // no process information throws away the call
  }

  it should "reduce a call using handleCall" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(PiState() withProc proc1 handleCall (PiCall < ("PROC", "R", "X")))
    ) //proc1.getFuture(Chan("X"),Chan("R")) ))
  }

  it should "reduce a call using proc.getFuture and proc.getInputs" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(
        PiState() withProc proc1 withCalls proc1.getFuture(1, Chan("R"), Chan("X")) withTerms proc1
              .getInputs(1, Chan("R"), Chan("X")) incFCtr ()
      )
    )
  }

  it should "reduce a call and introduce the correct inputs into the state" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(
        PiState(Devour("X", Chan("INPUT", 1))) withProc proc1 withCalls proc1.getFuture(
              1,
              Chan("R"),
              Chan("X")
            ) incFCtr ()
      )
    )
  }

  it should "reduce a input -> call -> processInput sequence: creating a thread" in {
    PiState() withProc proc1 withCalls proc1.getFuture(
      1,
      Chan("R"),
      Chan("X")
    ) withSub (Chan("INPUT", 1), PiItem("OHHAI!")) reduce () should be(
      Some(
        PiState() withProc proc1 withThread (0, "PROC", "R", Seq(
              PiResource(PiItem("OHHAI!"), Chan("X"))
            )) incTCtr
      )
    )
  }

  it should "reduce a input -> call -> processInput sequence: devour the input" in {
    PiState(Devour("X", "INPUT"), Out("X", PiItem("OHHAI!"))) withProc proc1 withCalls proc1
      .getFuture(1, Chan("R"), Chan("X")) reduce () should be(
      Some(
        PiState() withProc proc1 withCalls proc1.getFuture(
              1,
              Chan("R"),
              Chan("X")
            ) withSub ("INPUT", PiItem("OHHAI!"))
      )
    )
  }

  it should "reduce a input -> call -> processInput sequence: devour and create the thread" in {
    PiState(
      Devour("X", Chan("INPUT", 1)),
      Out("X", PiItem("OHHAI!"))
    ) withProc proc1 withCalls proc1
      .getFuture(1, Chan("R"), Chan("X")) fullReduce () should be(
      PiState() withProc proc1 withThread (0, "PROC", "R", Seq(
            PiResource(PiItem("OHHAI!"), Chan("X"))
          )) incTCtr
    )
  }

  it should "register a simple thread result properly" in {
    PiState() withThread (0, "PROC", "R", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X"))
    )) result (0, PiItem("!IAHHO")) should be(Some(PiState(Out("R", PiItem("!IAHHO")))))
  }

  it should "register a paired thread result properly" in {
    PiState() withThread (0, "PROC", "R", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X"))
    )) result (0, PiObject(("1", "2"))) should be(
      Some(PiState(ParOut("R", "R#L", "R#R", Out("R#L", PiItem("1")), Out("R#R", PiItem("2")))))
    )
    //TODO left and right options
  }

  it should "register a result to the correct thread" in {
    PiState() withThread (0, "PROC1", "R1", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X1"))
    )) withThread (1, "PROC2", "R2", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X2"))
    )) result (0, PiObject("!IAHHO")) should be(
      Some(
        PiState(Out("R1", PiItem("!IAHHO"))) withThread (1, "PROC2", "R2", Seq(
              PiResource(PiItem("OHHAI!"), Chan("X2"))
            ))
      )
    )
  }

  it should "register a result to the correct thread when it's not at the head" in {
    PiState() withThread (0, "PROC1", "R1", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X1"))
    )) withThread (1, "PROC2", "R2", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X2"))
    )) result (1, PiObject("!IAHHO")) should be(
      Some(
        PiState(Out("R2", PiItem("!IAHHO"))) withThread (0, "PROC1", "R1", Seq(
              PiResource(PiItem("OHHAI!"), Chan("X1"))
            ))
      )
    )
  }

  it should "fully execute a simple process" in {
    val proc1 = DummyProcess("PROC", "R", Seq((Chan("INPUT"), "C")))

    PiState(
      Out("C", PiItem("INPUT")),
      Devour("R", Chan("RESULT", 1))
    ) withProc proc1 withTerm (PiCall < ("PROC", "R", "C")) fullReduce () result (0, PiObject(
      "ResultString"
    )) map (_.fullReduce) should be(
      Some(
        PiState() withProc proc1 withSub (Chan("RESULT", 1), PiItem(
              "ResultString"
            )) incTCtr () incFCtr ()
      )
    )
  }
}

/* X->A + A->Y = X->Y `Pa (cPa_X_1,oPa_A_) =
 * Comp (In cPa_X_1 [cPa_X_1__a_X] Zero) (Res [oPa_A___a_A] (Out oPa_A_ [oPa_A___a_A] Zero))`
 *
 * `Pa (cPa_X_1,oPa_A_) =
 * Comp (In cPa_X_1 [cPa_X_1__a_X] Zero) (Res [oPa_A___a_A] (Out oPa_A_ [oPa_A___a_A] Zero))`
 *
 * `R1 (cPa_X_1,oPb_Y_) =
 * CutProc A z2 cPb_A_1 oPa_A_ (Pb (cPb_A_1,oPb_Y_)) (Pa (cPa_X_1,oPa_A_))`
 *
 * X->A**B + A->Y = X->Y**B
 *
 * `Pa (cPa_X_1,oPa___) =
 * Comp (In cPa_X_1 [cPa_X_1__a_X] Zero) (Res [oPa____A; oPa____B] (Out oPa___ [oPa____A; oPa____B]
 * (Comp (Res [oPa____l_a_A] (Out oPa____A [oPa____l_a_A] Zero)) (Res [oPa____r_a_B] (Out oPa____B
 * [oPa____r_a_B] Zero)))))`
 *
 * `Pb (cPb_A_1,oPb_Y_) =
 * Comp (In cPb_A_1 [cPb_A_1__a_A] Zero) (Res [oPb_Y___a_Y] (Out oPb_Y_ [oPb_Y___a_Y] Zero))`
 *
 * `R1 (cPa_X_1,z5) =
 * CutProc (A ** B) z8 z7 oPa___ (ParProc (NEG A) (NEG B) z7 cPb_A_1 buf5 (TimesProc Y B z5 oPb_Y_
 * b5 (Pb (cPb_A_1,oPb_Y_)) (IdProc B buf5 b5 m6))) (Pa (cPa_X_1,oPa___))` */
