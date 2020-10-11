package com.workflowfm.pew

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AtomicCallTests extends FlatSpec with Matchers with PiStateTester {
  it should "reduce a call after a simple input" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))

    reduceOnce(In("A", "C", PiCall < ("PROC", "C", "R")), Out("A", Chan("X"))) should be(
      Some(PiState())
    ) // no process information throws away the call

    PiState(
      In("A", "C", PiCall < ("PROC", "C", "R")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(PiState() withProc proc1 handleCall (PiCall < ("PROC", "X", "R")))
    ) //proc1.getFuture(Chan("X"),Chan("R")) ))

    PiState(
      In("A", "C", PiCall < ("PROC", "C", "R")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(
        PiState() withProc proc1 withCalls proc1.getFuture(0, Chan("X"), Chan("R")) withTerms proc1
              .getInputs(0, Chan("X"), Chan("R")) incFCtr ()
      )
    )

    PiState(
      In("A", "C", PiCall < ("PROC", "C", "R")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(
        PiState(Devour("X", "INPUT#0")) withProc proc1 withCalls proc1.getFuture(
              0,
              Chan("X"),
              Chan("R")
            ) incFCtr ()
      )
    )
  }

  it should "reduce a input -> call -> processInput" in {
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))

    PiState() withProc proc1 withCalls proc1.getFuture(
      0,
      Chan("X"),
      Chan("R")
    ) withSub ("INPUT#0", PiItem("OHHAI!")) reduce () should be(
      Some(
        PiState() withProc proc1 withThread (0, "PROC", "R", Seq(
              PiResource(PiItem("OHHAI!"), Chan("X"))
            )) incTCtr
      )
    )

    PiState(Devour("X", "INPUT"), Out("X", PiItem("OHHAI!"))) withProc proc1 withCalls proc1
      .getFuture(0, Chan("X"), Chan("R")) reduce () should be(
      Some(
        PiState() withProc proc1 withCalls proc1.getFuture(
              0,
              Chan("X"),
              Chan("R")
            ) withSub ("INPUT", PiItem("OHHAI!"))
      )
    )

    PiState(Devour("X", "INPUT#0"), Out("X", PiItem("OHHAI!"))) withProc proc1 withCalls proc1
      .getFuture(0, Chan("X"), Chan("R")) fullReduce () should be(
      PiState() withProc proc1 withThread (0, "PROC", "R", Seq(
            PiResource(PiItem("OHHAI!"), Chan("X"))
          )) incTCtr
    )
  }

  it should "register a thread result properly" in {
    PiState() withThread (0, "PROC", "R", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X"))
    )) result (0, PiItem("!IAHHO")) should be(Some(PiState(Out("R", PiItem("!IAHHO")))))
    PiState() withThread (0, "PROC", "R", Seq(
      PiResource(PiItem("OHHAI!"), Chan("X"))
    )) result (0, PiObject(("1", "2"))) should be(
      Some(PiState(ParOut("R", "R#L", "R#R", Out("R#L", PiItem("1")), Out("R#R", PiItem("2")))))
    )
    //TODO left and right options

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
    val proc1 = DummyProcess("PROC", Seq("C", "R"), "R", Seq((Chan("INPUT"), "C")))

    PiState(
      Out("C", PiItem("INPUT")),
      Devour("R", "RESULT#0")
    ) withProc proc1 withTerm (PiCall < ("PROC", "C", "R")) fullReduce () result (0, PiObject(
      "ResultString"
    )) map (_.fullReduce) should be(
      Some(
        PiState() withProc proc1 withSub ("RESULT#0", PiItem("ResultString")) incTCtr () incFCtr ()
      )
    )
  }
}

/*  X->A + A->Y = X->Y `Pa (cPa_X_1,oPa_A_) =
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
