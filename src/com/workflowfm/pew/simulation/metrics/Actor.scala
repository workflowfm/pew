package com.workflowfm.pew.simulation.metrics

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.pew.simulation.Simulation
import com.workflowfm.pew.simulation.Coordinator


object SimMetricsActor {
  case class Start(coordinator:ActorRef)
  case class StartSims(coordinator:ActorRef,sims:Seq[(Long,Simulation)],executor:SimulatorExecutor[_])
  case class StartSimsNow(coordinator:ActorRef,sims:Seq[Simulation],executor:SimulatorExecutor[_])
  
  def props(m:SimMetricsOutput, callbackActor:Option[ActorRef]=None)(implicit system: ActorSystem): Props = Props(new SimMetricsActor(m,callbackActor)(system))
}

// Provide a callbackActor to get a response when we are done. Otherwise we'll shutdown the ActorSystem 

class SimMetricsActor(m:SimMetricsOutput, callbackActor:Option[ActorRef])(implicit system: ActorSystem) extends Actor {
  var coordinator:Option[ActorRef] = None
  
  def receive = {
    case SimMetricsActor.Start(coordinator) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.Start
    }
    
    case SimMetricsActor.StartSims(coordinator,sims,executor) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.AddSims(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case SimMetricsActor.StartSimsNow(coordinator,sims,executor) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.AddSimsNow(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case Coordinator.Done(t:Long,ma:SimMetricsAggregator) if this.coordinator == Some(sender) => {
      this.coordinator = None
      m(t,ma)
      callbackActor match {
        case None => system.terminate()
        case Some(actor) => actor ! Coordinator.Done(t,ma)
      }
    }
  }
}
