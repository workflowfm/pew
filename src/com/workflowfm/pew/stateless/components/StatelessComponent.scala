package com.workflowfm.pew.stateless.components

import org.slf4j.{Logger, LoggerFactory}

abstract class StatelessComponent[In, Out]
  extends Function[In, Out] {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def respond: In => Out

  override def apply( in: In ): Out = respond( in )
}