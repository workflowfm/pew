package com.workflowfm.pew.simulator

import akka.actor.{ ActorRef, Props }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.{ SimulatedProcess, SimulationActor }
import scala.concurrent.{ ExecutionContext, Future }

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true
}

abstract class PiSimulationActor[T] (override val name: String, override val coordinator: ActorRef)
  (implicit executionContext: ExecutionContext)
    extends SimulationActor(name, coordinator) {

  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList

  def executor: SimulatorExecutor[T]
  val factory = new PiSimHandlerFactory[T](this)

  override def run(): Future[Any] = {
    executor.call(rootProcess, args, factory) flatMap (_.future)
  }

  def simulationCheck = if (executor.simulationReady) ready()
}

