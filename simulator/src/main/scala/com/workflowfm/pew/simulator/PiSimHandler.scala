package com.workflowfm.pew.simulator

import com.workflowfm.pew.stream.{ PiEventHandlerFactory, ResultHandler }
import com.workflowfm.pew.{ PiEvent, PiEventIdle, PiEventResult, PiFailure }

class PiSimHandler[T](actor: PiSimulationActor[T], id: T) extends ResultHandler[T](id) {
  override def apply(e: PiEvent[T]) = {
    e match {
      case PiEventIdle(i,_) if (i.id == id) => actor.simulationCheck
      case _ => Unit
    }
    super.apply(e)
  }
}

class PiSimHandlerFactory[T](actor: PiSimulationActor[T]) extends PiEventHandlerFactory[T,PiSimHandler[T]] {
  override def build(id: T) = new PiSimHandler[T](actor, id)
}
