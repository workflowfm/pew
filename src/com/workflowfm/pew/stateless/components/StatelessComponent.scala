package com.workflowfm.pew.stateless.components

abstract class StatelessComponent[In, Out]
  extends Function[In, Out] {

  def respond: In => Out

  override def apply( in: In ): Out = respond( in )
}