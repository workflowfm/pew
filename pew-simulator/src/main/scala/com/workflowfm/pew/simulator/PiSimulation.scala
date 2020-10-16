package com.workflowfm.pew.simulator

import java.util.UUID

import scala.collection.mutable.{ Map, Queue }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor.{ ActorRef, Props }

import com.workflowfm.pew.{
  PiProcess,
  PiInstance,
  PiInstanceStore,
  SimpleInstanceStore,
  PiEvent,
  PiEventIdle
}
import com.workflowfm.pew.execution.AkkaExecutorActor
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory, PrintEventHandler }
import com.workflowfm.simulator.{ Coordinator, Task }
import com.workflowfm.simulator.{ SimulatedProcess, AsyncSimulation, TaskGenerator }

abstract class PiSimulation(name: String, coordinator: ActorRef)(
    implicit override val executionContext: ExecutionContext
) extends AsyncSimulation(name, coordinator)
    with AkkaExecutorActor {

  override var store: PiInstanceStore[UUID] = SimpleInstanceStore[UUID]()

  implicit override val timeout: FiniteDuration = 10.seconds

  var computing: Seq[String] = Seq[String]()
  val waiting: Queue[String] = Queue[String]()
  val sources: Map[UUID, String] = Map()

  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList

  override def run(): Future[Any] = {
    //subscribe(new PrintEventHandler).flatMap(_ => )
    execute(rootProcess, args)
  }

  def readyCheck(): Unit = {
    val q = waiting.clone()
    val check = computing.forall { p =>
      q.dequeueFirst(_ == p) match {
        case None => false
        case Some(_) => true
      }
    }
    if (check) ready()
  }

  def processWaiting(process: String): Unit = {
    waiting += process
    readyCheck()
  }

  def processResuming(process: String): Unit = {
    waiting.dequeueFirst(_ == process)
  }

  override def complete(task: Task, time: Long): Unit = {
    sources.get(task.id).map(processResuming)
    super.complete(task, time)
  }

  override def publish(evt: PiEvent[UUID]): Unit = {
    super.publish(evt)
    evt match {
      case PiEventIdle(i, _) => executorReady(i)
      case _ => Unit
    }
  }

  def executorReady(i: PiInstance[_]): Unit = {
    val procs = i.getCalledProcesses
    if (procs.forall(_.isInstanceOf[PiSimulatedProcess])) {
      computing = procs map (_.iname)
      readyCheck()
    }
  }

  def piActorReceive: Receive = {
    case PiSimulation.Waiting(p) => {
      processWaiting(p)
      sender() ! PiSimulation.Ack
    }

    case PiSimulation.Resuming(p) => {
      processResuming(p)
      coordinator.forward(Coordinator.WaitFor(self))
    }
    case PiSimulation.AddTask(iname, t, resources) => {
      val id = java.util.UUID.randomUUID
      sources += id -> iname
      task(id, t, actorCallback(sender), resources)
    }
  }

  def wtfReceive: Receive = {
    case m => println(s"WTF WTF WTF WTF WTF WTF WTF WTF : $m")
  }

  override def receive: PartialFunction[Any, Unit] =
    publisherBehaviour orElse akkaReceive orElse simulationReceive orElse piActorReceive orElse wtfReceive
}

object PiSimulation {
  case class Waiting(process: String)
  case class Resuming(process: String)
  case object Ack
  case class AddTask(iname: String, t: TaskGenerator, resources: Seq[String])
}
