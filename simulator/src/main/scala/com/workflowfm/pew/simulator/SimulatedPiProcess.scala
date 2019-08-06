package com.workflowfm.pew.simulator

import akka.actor.{ ActorRef, Props }
import com.workflowfm.pew.{ MetadataAtomicProcess, PiInstance, PiMetadata, PiObject }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.simulator.{ SimulatedProcess, Task, TaskGenerator }
import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true

  override val iname = s"$simulationName.$name"

  def virtualWait() = simulationActor ! PiSimulationActor.Waiting(iname)
  def virtualResume() = simulationActor ! PiSimulationActor.Resuming(iname)

  override def simulate[T](
    gen: TaskGenerator,
    result: (Task, Long) => T,
    resources: String*
  )(implicit executionContext: ExecutionContext): Future[T] = {
    val id = java.util.UUID.randomUUID
    simulationActor ! PiSimulationActor.AddSource(id,iname)
    val f = simulate(id,gen,result,resources:_*)
    virtualWait()
    f
  }

}
