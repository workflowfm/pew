package com.workflowfm.pew.simulation.metrics

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.pew.simulation.Simulation
import com.workflowfm.pew.simulation.Coordinator


// Provide a callbackActor to get a response when we are done. Otherwise we'll shutdown the ActorSystem 
/** Interacts with a [[Coordinator]] to run simulations and apply a [[SimMetricsOutput]] when they are done.
  * @param m the [[SimMetricsOutput]] that will be applied to the metrics from every simulation [[Coordinator]] when it is done
  * @param callbackActor optional argument for a callback actor that we can forward the [[Coordinator.Done]] message. If this is [[scala.None]], we do not forward the message, but we shutdown the actor system, as we assume all simulations are completed.
  */
class SimMetricsActor(m:SimMetricsOutput, callbackActor:Option[ActorRef])(implicit system: ActorSystem) extends Actor {

  def receive = {
    case SimMetricsActor.Start(coordinator) => {
      coordinator ! Coordinator.Start
    }
    
    case SimMetricsActor.StartSims(coordinator,sims,executor) => {
      coordinator ! Coordinator.AddSims(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case SimMetricsActor.StartSimsNow(coordinator,sims,executor) => {
      coordinator ! Coordinator.AddSimsNow(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case Coordinator.Done(t:Long,ma:SimMetricsAggregator) => {
      m(t,ma)
      callbackActor match {
        case None => system.terminate()
        case Some(actor) => actor ! Coordinator.Done(t,ma)
      }
    }
  }
}

/** Contains the messages involved in [[SimMetricsActor]] and the [[akka.actor.Props]] initializer. */
object SimMetricsActor {
  case class Start(coordinator:ActorRef)
  case class StartSims(coordinator:ActorRef,sims:Seq[(Long,Simulation)],executor:SimulatorExecutor[_])
  case class StartSimsNow(coordinator:ActorRef,sims:Seq[Simulation],executor:SimulatorExecutor[_])
  
  def props(m:SimMetricsOutput, callbackActor:Option[ActorRef]=None)(implicit system: ActorSystem): Props = Props(new SimMetricsActor(m,callbackActor)(system))
}
