package com.workflowfm.pew.execution

import com.workflowfm.pew._
import com.workflowfm.pew.stream.{ PiObservable, PiEventHandler, PiEventHandlerFactory, ResultHandlerFactory }
import scala.concurrent._
import scala.concurrent.duration._

/** Executes an atomic process - blocking
  *
  * Built before the normal executors and kept around for testing.
  *
  * @deprecated
  */
case class AtomicProcessExecutor(process:AtomicProcess) {
  def call(args:PiObject*)(implicit ec:ExecutionContext) = {
    val s = process.execState(args) fullReduce()
    s.threads.headOption match {
      case None => None
      case Some((ref,PiFuture(_, _, args))) => {
        val f = process.run(args map (_.obj))
        val res = Await.result(f,Duration.Inf)
        s.result(ref,res) map (_.fullReduce()) map { x => PiObject.get(x.resources.sub(process.output._1)) }
      }
    }
  }
}

/**
 * Trait representing the ability to execute any PiProcess
 */
trait ProcessExecutor[KeyT] { this:PiObservable[KeyT] =>
  /**
    * Initializes a PiInstance for a process execution.
    * This is always and only invoked before a {@code start}, hence why it is protected.
    * This separation gives a chance to PiEventHandlers to subscribe before execution starts.
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The PiObject arguments to be passed to the process
    * @return A Future with the new unique ID that was generated
    */
  protected def init(process:PiProcess,args:Seq[PiObject]):Future[KeyT]

  /**
    * Starts the execution of an initialized PiInstance.
    * This is always and only invoked after an {@code init}, hence why it is protected.
    * This separation gives a chance to PiEventHandlers to subscribe before execution starts.
    * @param id The ID of the instance to start executing
    */
  protected def start(id:KeyT):Unit

  implicit val executionContext: ExecutionContext

  /**
    * A simple {@code init ; start} sequence when we do not need any even listeners.
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The (real) arguments to be passed to the process
    * @return A Future with the ID corresponding to this execution
    */
  def call(process:PiProcess,args:Seq[Any]):Future[KeyT] = {
    init(process,args map PiObject.apply) map { id => start(id) ; id }
  }

  /**
    * A {@code init ; start} sequence that gives us a chance to subscribe a listener
    * that is specific to this execution.
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The (real) arguments to be passed to the process
    * @param factory A PiEventHandlerFactory which generates PiEventHandler's for a given ID
    * @return A Future with the PiEventHandler that was generated
    */
  def call[H <: PiEventHandler[KeyT]](process:PiProcess,args:Seq[Any],factory:PiEventHandlerFactory[KeyT,H]):Future[H] = {
    init(process,args map PiObject.apply) flatMap { id =>
      val handler = factory.build(id)
      subscribe(handler).map {  _ =>
        start(id)
        handler
      }
    }
  }

  /**
    * Executes a process with a PromiseHandler
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The (real) arguments to be passed to the process
    * @return A Future with the result of the executed process
    */
  def execute(process:PiProcess,args:Seq[Any]):Future[Any] =
    call(process,args,new ResultHandlerFactory[KeyT]({ id => s"[$id]"})) flatMap (_.future)
}

object ProcessExecutor {
  final case class AlreadyExecutingException(private val cause: Throwable = None.orNull)
                    extends Exception("Unable to execute more than one process at a time", cause)
}

trait SimulatorExecutor[KeyT] extends ProcessExecutor[KeyT] { this:PiObservable[KeyT] =>
  /**
    *  This should check all executing PiInstances if they are simulationReady.
    *  This means that all possible execution has been performed and they are all
    *  waiting for simulation time to pass.
    *  @return true if all PiInstances are simulationReady
    */
  def simulationReady:Boolean

  /**
    * Executes a process with a ResultHandler
    * Same as ProcessExecutor.execute but blocks until call has been initiated.
    * The simulator needs to ensure this has happened before continuing.
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The (real) arguments to be passed to the process
    * @return A Future with the result of the executed process
    */
  def simulate(process:PiProcess,args:Seq[Any],timeout:FiniteDuration=10.seconds):Future[Any] = {
    val f = call(process,args,new ResultHandlerFactory[KeyT]({ id => s"[$id]"}))
    val handler = Await.result(f, timeout)
    handler.future
  }
}
