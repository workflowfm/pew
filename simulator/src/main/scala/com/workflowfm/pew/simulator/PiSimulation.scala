package com.workflowfm.pew.simulator

import akka.actor.{ ActorRef, Props }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.{ SimulatedProcess, Simulation, SimulationActor }
import scala.concurrent.{ ExecutionContext, Future }

/*
trait SimulatedProcess {
  def simulationActor: Actor

  def simulate[T](
    gen: TaskGenerator,
    result:TaskMetrics => T,
    resources:String*
  )(implicit executionContext: ExecutionContext):Future[T] = {
//    simulationActor ! SimulationActor.AddTask(gen, resources:_*).map(m => result(m))
  }
}
 */
trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true
}

abstract class PiSimulation (override val name: String, override val coordinator: ActorRef) extends Simulation(name, coordinator) {
  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList
}

class PiSimulationActor[T](override val simulation: PiSimulation, executor: SimulatorExecutor[T])
  (override implicit val executionContext: ExecutionContext)
    extends SimulationActor {

  val factory = new PiSimHandlerFactory[T](this)

  override def run(): Future[Any] = {
    executor.call(simulation.rootProcess, simulation.args, factory) flatMap (_.future)
  }

  def simulationCheck = if (executor.simulationReady) ready()
}
object PiSimulationActor {
  def props[T](simulation: PiSimulation, executor: SimulatorExecutor[T])
  (implicit executionContext: ExecutionContext): Props =
    Props(new PiSimulationActor[T](simulation, executor))
}

