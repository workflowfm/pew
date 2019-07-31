package com.workflowfm.pew.simulator

import akka.actor.{ ActorRef, Props }
import com.workflowfm.pew.{ MetadataAtomicProcess, PiInstance, PiMetadata, PiObject }
import com.workflowfm.pew.stream.{ PiEventHandler, PiEventHandlerFactory }
import com.workflowfm.pew.{ AtomicProcess, PiProcess }
import com.workflowfm.pew.execution.SimulatorExecutor
import com.workflowfm.simulator.metrics.TaskMetrics
import com.workflowfm.simulator.{ SimulatedProcess, SimulationActor, TaskGenerator }
import java.util.UUID
import scala.collection.mutable.{ Map, Queue }
import scala.concurrent.{ ExecutionContext, Future }

trait SimulatedPiProcess extends AtomicProcess with SimulatedProcess {
  override def isSimulatedProcess = true

  override val iname = s"$simulationName.$name"

  def ready() = simulationActor ! PiSimulationActor.Waiting(iname)

  override def simulate[T](
    gen: TaskGenerator,
    result:TaskMetrics => T,
    resources:String*
  )(implicit executionContext: ExecutionContext):Future[T] = {
    val id = java.util.UUID.randomUUID
    simulationActor ! PiSimulationActor.AddSource(id,iname)
    val f = simulate(id,gen,result,resources:_*)
    ready()
    f
  }

}

abstract class PiSimulationActor[T] (override val name: String, override val coordinator: ActorRef)
  (implicit executionContext: ExecutionContext)
    extends SimulationActor(name, coordinator) {

  var executorIsReady = true
  var waiting: Seq[String] = Seq[String]()
  val taskWaiting: Queue[String] = Queue[String]()
  val sources: Map[UUID,String] = Map()

  def rootProcess: PiProcess
  def args: Seq[Any]

  def getProcesses(): Seq[PiProcess] = rootProcess :: rootProcess.allDependencies.toList

  def executor: SimulatorExecutor[T]
  val factory = new PiSimHandlerFactory[T](self)

  override def run(): Future[Any] = {
    executorIsReady = false
    executor.call(rootProcess, args, factory) flatMap (_.future)
  }

  def readyCheck() = {
    val q = taskWaiting.clone()
    val check = waiting.forall { p =>
      q.dequeueFirst(_ == p) match {
        case None => false
        case Some(_) => true
      }
    }
    println(s"Check: $executorIsReady && $waiting && $taskWaiting = $check == ${executorIsReady & check}")
    if (executorIsReady && check) ready()
  }
//  def wait(process: String) = {
 //   println(s"Wait: $process")
  //  waiting += process
 // }
  
  def processWaiting(process: String) = {
    println(s"Process waiting: $process")
    if (!waiting.contains(process)) executorIsReady=false
    taskWaiting += process
    readyCheck()
  }

  override def complete(id: UUID, metrics: TaskMetrics) = {
    sources.get(id).map { p =>
      taskWaiting.dequeueFirst(_ == p)
    }
    super.complete(id,metrics)
  }

  def executorReady(i: PiInstance[_]) = {
    val procs = i.getCalledProcesses
    if (procs.forall(_.isSimulatedProcess)) {
      waiting = procs map (_.iname)
      executorIsReady = true
      readyCheck()
    }
  }

  /*
  def processDone(process: String) = {
    //println(s"DCheck: $waiting")
    if (!waiting.contains(process)) println(s" ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR    $process     ERROR ERROR ERROR ERROR ERROR ERROR ERROR ")
    else println(s"Done: $process")
    taskWaiting.dequeueFirst( _ == process )
    waiting.dequeueFirst(_ == process)
    readyCheck()
  }
   */
  def executorBusy() = executorIsReady = false

  def piActorReceive: Receive = {
    case PiSimulationActor.Waiting(p) => processWaiting(p)
    case PiSimulationActor.AddSource(id,iname) => sources += id->iname
    case PiSimulationActor.ExecutorBusy => executorBusy()
    case PiSimulationActor.ExecutorReady(i) => executorReady(i)
  }

  override def receive = piActorReceive orElse simulationActorReceive
}
object PiSimulationActor {
  case class Waiting(process: String)
  case class AddSource(id: UUID, iname: String)
  case object ExecutorBusy
  case class ExecutorReady(i: PiInstance[_])
}
