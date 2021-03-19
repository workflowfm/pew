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
import com.workflowfm.pew.simulator.PiSimulation
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.proter.{ Task, TaskInstance }

trait PiSimulatedProcess extends AtomicProcess {
  def simulation: PiSimulation

  def simulate[T](
      gen: Task,
      resultFun: (TaskInstance, Long) => T
  ): Future[T] = {
    simulation.simulate(iname, gen, resultFun)
  }

  def simulate[T](
      iname: String,
      gen: Task,
      result: T
  ): Future[T] = {
    simulation.simulate(iname, gen, result)
  }

  def waiting(): Unit = {
    simulation.processWaiting(iname)
  }

  def resume(): Unit = {
    simulation.processResuming(iname)
  }

}
