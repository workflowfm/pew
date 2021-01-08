package com.workflowfm.pew

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositeCallTests extends FlatSpec with Matchers with PiStateTester {
    val proc1 = DummyComposition("PROC", "A", "B", "V")

  it should "reduce a composite call using handleCall" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(PiState() withProc proc1 handleCall (PiCall < ("PROC", "R", "X")))
    ) //proc1.getFuture(Chan("X"),Chan("R")) ))
  }

  it should "reduce a composite call adding the body as a term" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(PiState() withProc proc1 withTerm PiId("X", "R", "V#1") incFCtr ())
    )
  }

/* proc.call does not freshen the body 
  it should "reduce a composite call adding proc.call as a term" in {
    PiState(
      In("A", "C", PiCall < ("PROC", "R", "C")),
      Out("A", Chan("X"))
    ) withProc proc1 reduce () should be(
      Some(PiState() withProc proc1 withTerm proc1.call(Chan("X"), Chan("R")) incFCtr ())
    )
  }
 */
  it should "fully execute a dummy composition" in {
    val proc1 = DummyComposition("PROC", "A", "B", "V")

    PiState(
      Out("C", PiItem("SOMETHING")),
      Devour("R", "RESULT")
    ) withProc proc1 withTerm (PiCall < ("PROC", "R", "C")) fullReduce () should be(
      PiState() withProc proc1 withSub ("RESULT", PiItem("SOMETHING")) incFCtr ()
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
