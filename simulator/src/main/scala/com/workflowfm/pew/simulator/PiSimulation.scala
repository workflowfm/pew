package com.workflowfm.pew.simulator

import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.{ SimulatedProcess, Simulation }
import scala.concurrent.Future

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true
}

abstract class PiSimulation (val name: String) {
  def run(executor: SimulatorExecutor[_]): Future[Any]
  def getProcesses(): Seq[PiProcess]
  def simulateWith(executor: SimulatorExecutor[_]) :Simulation = new PiSimulationInstance(this, executor)
}

class PiSimulationInstance(sim: PiSimulation, executor: SimulatorExecutor[_]) extends Simulation(sim.name) {
  override def run(): Future[Any] = sim.run(executor)
  override def simulationReady = executor.simulationReady
}

