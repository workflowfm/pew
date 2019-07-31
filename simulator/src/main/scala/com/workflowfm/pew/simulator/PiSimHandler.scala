package com.workflowfm.pew.simulator

import akka.actor.ActorRef
import com.workflowfm.pew.stream.{ PiEventHandlerFactory, ResultHandler }
import com.workflowfm.pew.{ PiEvent, PiEventCall, PiEventReturn, PiEventIdle, PiEventResult, PiFailure }

class PiSimHandler[T](actor: ActorRef, id: T) extends ResultHandler[T](id) {
  override def apply(e: PiEvent[T]) = {
    e match {
      case PiEventIdle(i,_) if (i.id == id) => actor ! PiSimulationActor.ExecutorReady(i)
      case PiEventReturn(i,_,_,_) if (i == id) => actor ! PiSimulationActor.ExecutorBusy
      case _ => Unit
    }
    super.apply(e)
  }
}

class PiSimHandlerFactory[T](actor: ActorRef) extends PiEventHandlerFactory[T,PiSimHandler[T]] {
  override def build(id: T) = new PiSimHandler[T](actor, id)
}
