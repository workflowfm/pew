package com.workflowfm.pew.simulator

import java.util.UUID

import scala.collection.mutable.{ Map, Queue }
import scala.concurrent.{ ExecutionContext, Future }

import com.workflowfm.pew.{
  PiProcess,
  PiInstance,
  PiInstanceStore,
  SimpleInstanceStore,
  PiEvent,
  PiEventIdle,
}
import com.workflowfm.pew.execution.MutexExecutor
import com.workflowfm.proter.{ Manager, TaskInstance, AsyncSimulation, FutureTasks, Task }


abstract class PiSimulation(
  override val name: String, 
  override protected val manager: Manager
)(
  implicit override val executionContext: ExecutionContext
) extends MutexExecutor()(executionContext)
    with AsyncSimulation
    with FutureTasks {

  val procsWaiting: Queue[String] = Queue[String]()
  val sources: Map[UUID, String] = Map()

  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList

  override def run(): Unit = {
    //subscribe(new PrintEventHandler).flatMap(_ => )
    execute(rootProcess, args).onComplete(done)
  }

  def simulate[T](
    iname: String,
    gen: Task,
    resultFun: (TaskInstance, Long) => T
  ): Future[T] = {
    val id = gen.id.getOrElse(UUID.randomUUID())
    sources += id -> iname
    val f = futureTask(gen.withID(id)).map { case (task, time) => resultFun(task, time) }
    processWaiting(iname)
    f
  }

  def simulate[T](
    iname: String,
    gen: Task,
    result: T
  ): Future[T] = { 
    val f: (TaskInstance, Long) => T = { case (_, _) => result }
    simulate(iname, gen, f)
  }

  protected def readyCheck(): Unit = {
    val procs = store.getAll().flatMap(_.getCalledProcesses) // we should only have one instance though!
    if (procs.forall(_.isInstanceOf[PiSimulatedProcess])) {
      val computing = procs map (_.iname)
      val q = procsWaiting.clone()
      val check = computing.forall { p =>
        q.dequeueFirst(_ == p) match {
          case None => false
          case Some(_) => true
        }
      }
      if (check) ready()
    }
  }

  def processWaiting(process: String): Unit = {
    procsWaiting += process
    readyCheck()
  }

  def processResuming(process: String): Unit = {
    procsWaiting.dequeueFirst(_ == process)
  }

  override def completed(time: Long, tasks: Seq[TaskInstance]): Unit = {
    this.synchronized {
      tasks.map { task => sources.get(task.id).map(processResuming) }
    }
    super.completed(time, tasks)
  }

  override def publish(evt: PiEvent[Int]): Unit = {
    super.publish(evt)
    evt match {
      case PiEventIdle(i, _) => readyCheck()
      case _ => Unit
    }
  }
}
