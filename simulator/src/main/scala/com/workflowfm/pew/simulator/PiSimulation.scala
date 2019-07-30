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

abstract class PiSimulation (val name: String, val coordinator: ActorRef) {
  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList
}

class PiSimulationActor[T](implicit executionContext: ExecutionContext)
    extends SimulationActor(simulation.name, simulation.coordinator) {

  var executor: Option[SimulatorExecutor[T]] = None
  val factory = new PiSimHandlerFactory[T](this)

  override def run(): Future[Any] = executor match {
    case None => Future.failed(new RuntimeException(s"Tried to start simulation actor [${simulation.name}] with no executor."))
    case Some(exe) => exe.call(simulation.rootProcess, simulation.args, factory) flatMap (_.future)
  }

  def simulationCheck = if (executor.map(_.simulationReady).getOrElse(true)) ready()

  def piSimulatorReceive: Receive = {
    case PiSimulationActor.Init(exe) => {
      if (executor.isEmpty && exe.isInstanceOf[SimulatorExecutor[T]])
        executor = Some(exe.asInstanceOf[SimulatorExecutor[T]])
      sender() ! PiSimulationActor.Ack
  }}

  override def receive = piSimulatorReceive orElse piSimulatorReceive
}
object PiSimulationActor {
  case class Init[T](executor: SimulatorExecutor[T])
  case object Ack

  /*
  def props[T](simulation: PiSimulation)
  (implicit executionContext: ExecutionContext): Props =
    Props(new PiSimulationActor[T](simulation))
   */
}

