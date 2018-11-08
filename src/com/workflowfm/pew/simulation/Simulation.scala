package com.workflowfm.pew.simulation

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import com.workflowfm.pew.execution.AkkaExecutor
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import com.workflowfm.pew.PiProcess
import com.workflowfm.pew.execution.SimulatorExecutor


abstract class Simulation(val name:String)  { //extends SimulationMetricTracker
  def run(executor:SimulatorExecutor[_]):Future[Any]
  def getProcesses():Seq[PiProcess]
}

class TaskSimulation(simulationName:String, coordinator:ActorRef, resources:Seq[String], duration:ValueGenerator[Int]=new ConstantGenerator(1), val cost:ValueGenerator[Int]=new ConstantGenerator(1), interrupt:Int=(-1), priority:Task.Priority=Task.Medium)(implicit system: ActorSystem) extends Simulation(simulationName) {
  def run(executor:SimulatorExecutor[_]) = {
    TaskGenerator(simulationName + "Task", simulationName, duration, cost, interrupt, priority).addTo(coordinator,resources :_*)
	}
  override def getProcesses() = Seq()
}

class MockExecutor extends Actor {
  def receive = {
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
  }
}

object SimulationActor {
  case class Run(coordinator:ActorRef,executor:SimulatorExecutor[_])
  case object AckRun
  
  def props(s:Simulation)(implicit ec: ExecutionContext = ExecutionContext.global): Props = Props(new SimulationActor(s)(ec))//.withDispatcher("akka.my-dispatcher")
  def mockExecProps:Props = Props(new MockExecutor) // TODO do this properly with props.of ?
}
class SimulationActor(s:Simulation)(implicit ec: ExecutionContext) extends Actor {
  def receive = {
    case SimulationActor.Run(coordinator,executor) => { 
      //val coordinator = sender()
      s.run(executor).onComplete({
        case Success(res) => {
          coordinator ! Coordinator.SimDone(s.name,res.toString)
          println("*** Result of " + s.name + ": " + res)
          context.stop(self) 
        }
        case Failure(ex) => {
          coordinator ! Coordinator.SimDone(s.name,ex.getLocalizedMessage)
          println("*** Exception in " + s.name + ": ")
          ex.printStackTrace()
          context.stop(self)  
        }
      })
      coordinator ! SimulationActor.AckRun
    }
  }
}

trait SimulatedProcess { this:PiProcess =>
   def simulationName:String
   override def isSimulatedProcess = true
   
   def simulate[T](gen:TaskGenerator, coordinator:ActorRef, result:T, resources:String*)(implicit system: ActorSystem, context: ExecutionContext = ExecutionContext.global):Future[T] ={
     gen.addTo(coordinator, resources:_*).map(_ => result)
   }
}