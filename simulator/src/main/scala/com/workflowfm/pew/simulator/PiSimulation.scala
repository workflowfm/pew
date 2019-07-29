package com.workflowfm.pew.simulator

import akka.actor.ActorRef
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.{ SimulatedProcess, SimulationActor }
import scala.concurrent.{ ExecutionContext, Future }

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true
}

abstract class PiSimulation (val name: String, val coordinator: ActorRef) {
  def run(executor: SimulatorExecutor[_]): Future[Any]
  def getProcesses(): Seq[PiProcess]
  def simulateWith(executor: SimulatorExecutor[_])(implicit executionContext: ExecutionContext): PiSimulationActor =
    new PiSimulationActor(this, executor)
}

class PiSimulationActor(sim: PiSimulation, executor: SimulatorExecutor[_])
  (override implicit val executionContext: ExecutionContext)
    extends SimulationActor(sim.name, sim.coordinator) {
  override def run(): Future[Any] = sim.run(executor)
  def simulationReady = executor.simulationReady
}

