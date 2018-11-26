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

class TaskSimulation(simulationName:String, coordinator:ActorRef, resources:Seq[String], duration:ValueGenerator[Long], val cost:ValueGenerator[Long]=new ConstantGenerator(0L), interrupt:Int=(-1), priority:Task.Priority=Task.Medium)(implicit system: ActorSystem) extends Simulation(simulationName) {
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

trait SimulatedProcess { this:PiProcess =>
   def simulationName:String
   override def isSimulatedProcess = true
   
   def simulate[T](gen:TaskGenerator, coordinator:ActorRef, result:T, resources:String*)(implicit system: ActorSystem, context: ExecutionContext = ExecutionContext.global):Future[T] ={
     gen.addTo(coordinator, resources:_*).map(_ => result)
   }
}
