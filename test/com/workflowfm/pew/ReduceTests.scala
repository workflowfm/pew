package com.workflowfm.pew

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReduceTests extends FlatSpec with Matchers with PiStateTester {

  it should "not reduce the empty state" in {
		reduceOnce() should be( None )
	}

	it should "reduce simple I/O" in {
		reduce(Devour("C","V"),Out("C",PiItem("OUTPUT"))) should be( fState((Chan("V"),PiItem("OUTPUT"))) )
		reduce(Devour("D","W"),Out("C",PiItem("OUTPUT1")),Devour("C","V"),Out("D",PiItem("OUTPUT2"))) should be(
				fState((Chan("V"),PiItem("OUTPUT1")),
						(Chan("W"),PiItem("OUTPUT2"))) )
	}
	
	it should "reduce back-to-back input" in {
		reduce(In("A","C",Devour("C","V")),Out("A",Chan("X"))) should be( PiState(Devour("X","V")) )
		reduce(In("A","C",Devour("C","V")),Out("A",Chan("X")),Out("X",PiItem("OHHAI!"))) should be( fState((Chan("V"),PiItem("OHHAI!"))) )
	}
	
	it should "reduce a simple buffer" in {
    reduce(Out("IN",PiItem("HELLO")),In("IN","V",Out("OUT",Chan("V")))) should be( PiState(Out("OUT",PiItem("HELLO"))) )  
	  reduce(Devour("OUT","WUT"),Out("IN",PiItem("HELLO")),In("IN","V",Out("OUT",Chan("V")))) should be( fState((Chan("WUT"),PiItem("HELLO"))) )
		reduceGet("WUT",Devour("OUT","WUT"),Out("IN",PiItem("HELLO")),In("IN","V",Out("OUT",Chan("V")))) should be( PiItem("HELLO") )
	}

	it should "reduce a parallel I/O" in {
    reduce(ParIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),ParOut("X","A","B",Out("A",PiItem("Left")),Out("B",PiItem("Right")))) should be( fState((Chan("LEFT"),PiItem("Left")),(Chan("RIGHT"),PiItem("Right"))) incFCtr() )
	}
	
	it should "reduce a A*(B*C) I/O" in {
    reduce(ParIn("X","L","R",Devour("L","AAA"),ParIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),ParOut("X","A","Y",Out("A",PiItem("aaa")),ParOut("Y","B","C",Out("B",PiItem("bbb")),Out("C",PiItem("ccc"))))) should be( fState((Chan("AAA"),PiItem("aaa")),(Chan("BBB"),PiItem("bbb")),(Chan("CCC"),PiItem("ccc"))) withFCtr(2) )
	}

	it should "reduce a (B*C)*A I/O" in {
    reduce(ParIn("X","R","L",ParIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC")),Devour("L","AAA")),ParOut("X","Y","A",ParOut("Y","B","C",Out("B",PiItem("bbb")),Out("C",PiItem("ccc"))),Out("A",PiItem("aaa")))) should be( fState((Chan("AAA"),PiItem("aaa")),(Chan("BBB"),PiItem("bbb")),(Chan("CCC"),PiItem("ccc"))) withFCtr(2) )
	}
	
	it should "reduce an optional I/O" in {
    reduce(WithIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),LeftOut("X","A",Out("A",PiItem("Left")))) should be( fState((Chan("LEFT"),PiItem("Left"))) incFCtr() )
    reduce(WithIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),RightOut("X","B",Out("B",PiItem("Right")))) should be( fState((Chan("RIGHT"),PiItem("Right"))) incFCtr() )
	}
	
	it should "reduce a A+(B+C) I/O" in {
    reduce(WithIn("X","L","R",Devour("L","AAA"),WithIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),LeftOut("X","A",Out("A",PiItem("aaa")))) should be( fState((Chan("AAA"),PiItem("aaa"))) withFCtr(1))
    reduce(WithIn("X","L","R",Devour("L","AAA"),WithIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),RightOut("X","BC",LeftOut("BC","B",Out("B",PiItem("bbb"))))) should be( fState((Chan("BBB"),PiItem("bbb"))) withFCtr(2))
    reduce(WithIn("X","L","R",Devour("L","AAA"),WithIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),RightOut("X","BC",RightOut("BC","C",Out("C",PiItem("ccc"))))) should be( fState((Chan("CCC"),PiItem("ccc"))) withFCtr(2))
	}
	
  it should "not reduce non-admissible I/O" in {
		reduceOnce(ParIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),Out("X",PiItem("OUTPUT"))) should be( None )
    reduce(ParIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),Out("X",PiItem("OUTPUT")),Out("C",PiItem("OUTPUT")),Devour("C","V")) should be( 
      PiState(ParIn("X","L","R",Devour("L","LEFT"),Devour("R","RIGHT")),Out("X",PiItem("OUTPUT"))) withSub ("V",PiItem("OUTPUT"))     
    )
	}
}


