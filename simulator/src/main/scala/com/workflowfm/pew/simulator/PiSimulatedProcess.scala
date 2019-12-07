package com.workflowfm.pew.simulator

import akka.actor.{ ActorRef, Props }
import com.workflowfm.pew.{ MetadataAtomicProcess, PiInstance, PiMetadata, PiObject }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.simulator.{ SimulatedProcess, Task, TaskGenerator }
import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }
import akka.pattern.ask
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

trait PiSimulatedProcess extends AtomicProcess with SimulatedProcess {
  override val iname = s"$simulationName.$name"

  // TODO We never actually wait for these asks, so there is still a chance the ordering will be messed up
  // if the messages are delayed
  def virtualWait() = (simulationActor ? PiSimulationActor.Waiting(iname))(Timeout(1, TimeUnit.DAYS))
  def virtualResume() = (simulationActor ? PiSimulationActor.Resuming(iname))(Timeout(1, TimeUnit.DAYS))

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
