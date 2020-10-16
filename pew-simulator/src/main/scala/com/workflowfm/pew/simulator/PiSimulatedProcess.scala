package com.workflowfm.pew.simulator

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration

import akka.actor.{ ActorRef, Props }
import akka.pattern.ask
import akka.util.Timeout

import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.{ MetadataAtomicProcess, PiInstance, PiMetadata, PiObject }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.simulator.{ SimulatedProcess, Task, TaskGenerator }

trait PiSimulatedProcess extends AtomicProcess with SimulatedProcess {
  override val iname: String = s"$simulationName.$name"

  /* TODO We never actually wait for these asks, so there is still a chance the ordering will be
   * messed up */
  // if the messages are delayed
  override def simWait(): Unit =
    (simulationActor ? PiSimulation.Waiting(iname))(Timeout(1, TimeUnit.DAYS))

  def simResume(): Future[Any] =
    (simulationActor ? PiSimulation.Resuming(iname))(Timeout(1, TimeUnit.DAYS))

  override def simulate[T](
      gen: TaskGenerator,
      result: (Task, Long) => T,
      resources: String*
  )(implicit executionContext: ExecutionContext): Future[T] = {
    val f = (simulationActor ? PiSimulation.AddTask(iname, gen, resources))(
      Timeout(1, TimeUnit.DAYS)
    ).mapTo[(Task, Long)].map { case (task, time) => result(task, time) }
    simWait()
    f
  }

}
