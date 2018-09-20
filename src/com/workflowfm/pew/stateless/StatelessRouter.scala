package com.workflowfm.pew.stateless

trait StatelessRouter[MsgT] {
  def route( msg: Any ): MsgT
}

/** Object for elevating functions and closures into concrete StatelessRouter objects.
  *
  * @param baseFn Function to be called to route messages.
  */
object FnStatelessRouter {
  implicit def fromFn[MsgT]( baseFn: Any => MsgT): StatelessRouter[MsgT] = {
    new StatelessRouter[MsgT] {
      override def route(msg: Any): MsgT = baseFn(msg)
    }
  }

  val meh: StatelessRouter[Unit] = (_: Any) => println("")
}
