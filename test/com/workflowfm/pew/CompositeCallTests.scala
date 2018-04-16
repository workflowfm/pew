package com.workflowfm.pew

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositeCallTests extends FlatSpec with Matchers with PiStateTester {  
  it should "reduce a call after a simple input" in { 
    val proc1 = DummyComposition("PROC", "A", "B", "V")
    
    reduceOnce(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) should be( Some(PiState()) ) // no process information throws away the call
    
    PiState(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) withProc proc1 reduce() should be (Some(
        PiState() withProc proc1 handleCall(PiCall<("PROC","X","R")) ))//proc1.getFuture(Chan("X"),Chan("R")) ))

    PiState(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) withProc proc1 reduce() should be (Some(
        PiState() withProc proc1 withTerm PiId("X","R","V") incFCtr() ))
    
    PiState(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) withProc proc1 reduce() should be (Some(
        PiState() withProc proc1 withTerm proc1.call(Chan("X"),Chan("R")) incFCtr() ))
  }
  
  it should "fully execute a dummy composition" in {
    val proc1 = DummyComposition("PROC", "A", "B", "V")
    
    PiState(Out("C",PiItem("SOMETHING")),Devour("R","RESULT")) withProc proc1 withTerm (PiCall<("PROC","C","R")) fullReduce() should be (
        PiState() withProc proc1 withSub ("RESULT",PiItem("SOMETHING")) incFCtr() )
  }

}

/*
 * 
 * X->A + A->Y = X->Y
 `Pa (cPa_X_1,oPa_A_) =
   Comp (In cPa_X_1 [cPa_X_1__a_X] Zero)
   (Res [oPa_A___a_A] (Out oPa_A_ [oPa_A___a_A] Zero))`

 `Pa (cPa_X_1,oPa_A_) =
   Comp (In cPa_X_1 [cPa_X_1__a_X] Zero)
   (Res [oPa_A___a_A] (Out oPa_A_ [oPa_A___a_A] Zero))`

   `R1 (cPa_X_1,oPb_Y_) =
   CutProc A z2 cPb_A_1 oPa_A_ (Pb (cPb_A_1,oPb_Y_)) (Pa (cPa_X_1,oPa_A_))`




   X->A**B + A->Y = X->Y**B

   `Pa (cPa_X_1,oPa___) =
   Comp (In cPa_X_1 [cPa_X_1__a_X] Zero)
   (Res [oPa____A; oPa____B]
   (Out oPa___ [oPa____A; oPa____B]
   (Comp
    (Res [oPa____l_a_A]
    (Out oPa____A [oPa____l_a_A] Zero))
   (Res [oPa____r_a_B]
   (Out oPa____B [oPa____r_a_B] Zero)))))`

  `Pb (cPb_A_1,oPb_Y_) =
   Comp (In cPb_A_1 [cPb_A_1__a_A] Zero)
   (Res [oPb_Y___a_Y] (Out oPb_Y_ [oPb_Y___a_Y] Zero))`

  `R1 (cPa_X_1,z5) =
   CutProc (A ** B) z8 z7 oPa___
   (ParProc (NEG A) (NEG B) z7 cPb_A_1 buf5
   (TimesProc Y B z5 oPb_Y_ b5 (Pb (cPb_A_1,oPb_Y_)) (IdProc B buf5 b5 m6)))
   (Pa (cPa_X_1,oPa___))`
   
   
	
 */
