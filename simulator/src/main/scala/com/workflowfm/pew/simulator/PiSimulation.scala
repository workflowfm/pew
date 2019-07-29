package com.workflowfm.pew.simulator

import akka.actor.ActorRef
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.{ SimulatedProcess, SimulationActor }
import scala.concurrent.{ ExecutionContext, Future }

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true
}

abstract class PiSimulation (val name: String, val coordinator: ActorRef) {

  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList

  def simulateWith[T](executor: SimulatorExecutor[T])(implicit executionContext: ExecutionContext): PiSimulationActor[T] =
    new PiSimulationActor(this, executor)
}

class PiSimulationActor[T](sim: PiSimulation, executor: SimulatorExecutor[T])
  (override implicit val executionContext: ExecutionContext)
    extends SimulationActor(sim.name, sim.coordinator) {

  val factory = new PiSimHandlerFactory[T](this)

  override def run(): Future[Any] = {
    executor.call(sim.rootProcess, sim.args, factory) flatMap (_.future)
  }

  def simulationCheck = if (executor.simulationReady) ready()
}

