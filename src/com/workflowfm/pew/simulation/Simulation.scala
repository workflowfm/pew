package com.workflowfm.pew.simulation

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import com.workflowfm.pew.execution.AkkaExecutor
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext


abstract class Simulation(val name:String) extends SimulationMetricTracker {
  def run():Future[Any]
  def executor: ActorRef
}

class TaskSimulation(simulationName:String, coordinator:ActorRef, resources:Seq[String], duration:ValueGenerator[Int]=new ConstantGenerator(1), val cost:ValueGenerator[Int]=new ConstantGenerator(1), interrupt:Int=(-1), priority:Task.Priority=Task.Medium)(implicit system: ActorSystem) extends Simulation(simulationName) {
  override val executor = system.actorOf(SimulationActor.mockExecProps)
  def run() = {
    TaskGenerator(simulationName + "Task", simulationName, duration, cost, interrupt, priority).create(simulationName + "Result",resources :_*).addTo(coordinator)
	}
}

class MockExecutor extends Actor {
  def receive = {
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
  }
}

object SimulationActor {
  case class Run(coordinator:ActorRef)
  case object AckRun
  
  def props(s:Simulation): Props = Props(new SimulationActor(s))//.withDispatcher("akka.my-dispatcher")
  def mockExecProps:Props = Props(new MockExecutor) // TODO do this properly with props.of ?
}
class SimulationActor(s:Simulation)(implicit ec: ExecutionContext = ExecutionContext.global) extends Actor {
  def receive = {
    case SimulationActor.Run(coordinator) => { 
      //val coordinator = sender()
      s.run().onComplete({
        case Success(res) => {
          s.setResult(res.toString)
          coordinator ! Coordinator.SimDone(s)
          println("*** Result of " + s.name + ": " + res)
          context.stop(self) 
        }
        case Failure(ex) => {
          s.setResult(ex.getLocalizedMessage)
          coordinator ! Coordinator.SimDone(s)
          println("*** Exception in " + s.name + ": ")
          ex.printStackTrace()
          context.stop(self)  
        }
      })
      coordinator ! SimulationActor.AckRun
    }
  }
}

trait SimulatedProcess {
   def simulationName:String
}