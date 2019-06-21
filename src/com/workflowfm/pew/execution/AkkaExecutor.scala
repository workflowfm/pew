package com.workflowfm.pew.execution

import com.workflowfm.pew._

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class AkkaExecutor (store: PiInstanceStore[Int], processes: PiProcessStore)
                   (implicit val system: ActorSystem,
                    override implicit val executionContext: ExecutionContext = ExecutionContext.global,
                    implicit val timeout: FiniteDuration = 10.seconds)
  extends SimulatorExecutor[Int]
    with PiObservable[Int] {

  def this(store:PiInstanceStore[Int], l:PiProcess*)
          (implicit system: ActorSystem, context: ExecutionContext, timeout: FiniteDuration) =
    this(store,SimpleProcessStore(l :_*))

  def this(system: ActorSystem, context: ExecutionContext, timeout:FiniteDuration, l: PiProcess*) =
    this(SimpleInstanceStore[Int](), SimpleProcessStore(l :_*))(system, context, timeout)

  def this(l:PiProcess*)
          (implicit system: ActorSystem) =
    this(SimpleInstanceStore[Int](), SimpleProcessStore(l :_*))(system, ExecutionContext.global, 10.seconds)

  // Execution actor
  val execActor: ActorRef = system.actorOf(AkkaExecutor.execprops(store, processes))

  // Future await timeout
  implicit val tOut: Timeout = Timeout(timeout)

  override def simulationReady: Boolean =
    Await.result(execActor ? AkkaExecutor.SimReady, timeout)
      .asInstanceOf[Boolean]

  override protected def init(process:PiProcess, args:Seq[PiObject]): Future[Int] =
    execActor ? AkkaExecutor.Init(process, args) map (_.asInstanceOf[Int])

  override protected def start(id: Int): Unit =
    execActor ! AkkaExecutor.Start(id)

  override def subscribe(handler: PiEventHandler[Int]): Future[Boolean] =
    (execActor ? AkkaExecutor.Subscribe(handler))
      .mapTo[Boolean]

  override def unsubscribe(name: String): Future[Boolean] =
    (execActor ? AkkaExecutor.Unsubscribe(name))
      .mapTo[Boolean]
}

// Defines messages between the different actors involved in the execution
object AkkaExecutor {
  case class Init(p: PiProcess, args: Seq[PiObject])
  case class Start(id: Int)
  case class Result(id: Int, ref: Int, res: MetadataAtomicProcess.MetadataAtomicResult)
  case class Error(id: Int, ref: Int, ex: Throwable)

  case class ACall(id: Int, ref: Int, p: MetadataAtomicProcess, args: Seq[PiObject], actor: ActorRef)
  case object AckCall

  case class AFuture(f: Future[Any])

  case object Ping
  case object SimReady

  case class Subscribe(handler: PiEventHandler[Int])
  case class Unsubscribe(name: String)

  /**
    * Properties of an actor for an atomic process executor
    *
    * @param context Actor execution context
    * @return Atomic process executor actor properties
    */
  def atomicprops(implicit context: ExecutionContext = ExecutionContext.global): Props =
    Props(new AkkaAtomicProcessExecutor())

  /**
    * Properties of an actor for a process executor
    *
    * @param store Process instance store
    * @param processes Process store
    * @param system Akka actor system
    * @param exc Actor execution context
    * @return Process executor actor properties
    */
  def execprops(store: PiInstanceStore[Int], processes: PiProcessStore)
               (implicit system: ActorSystem, exc: ExecutionContext): Props =
    Props(new AkkaExecActor(store,processes))
}

/**
  * Process executor actor
  *
  * @param store Process instance store
  * @param processes Process store
  * @param system Akka actor system
  * @param executionContext Actor execution context
  */
class AkkaExecActor(var store: PiInstanceStore[Int], processes: PiProcessStore)
                   (implicit system: ActorSystem,
                    override implicit val executionContext: ExecutionContext = ExecutionContext.global)
  extends Actor
    with SimplePiObservable[Int] {

  var ctr: Int = 0

  /**
    * Initialize a workflow
    *
    * @param p Workflow process
    * @param args Instantiation arguments
    * @return ID of the workflow instance
    */
  def init(p: PiProcess, args: Seq[PiObject]): Int = {
    val inst = PiInstance(ctr, p, args:_*)
    store = store.put(inst)
    ctr = ctr + 1
    ctr-1
  }

  /**
    * Start a workflow instance
    * Publish [[PiFailureNoSuchInstance]] if no such instance found in the store
    *
    * @param id ID of the workflow instance
    */
  def start(id: Int): Unit = store.get(id) match {
    case None => publish(PiFailureNoSuchInstance(id))
    case Some(inst) =>
      publish(PiEventStart(inst))

      // Reduce the instance and check for completion
      val newInstance = inst.reduce
      if (newInstance.completed) {
        // Check for result and publish it (or the failure)
        newInstance.result match {
          case None =>
            publish(PiFailureNoResult(newInstance))
            store = store.del(id)
          case Some(res) =>
            publish(PiEventResult(newInstance, res))
            store = store.del(id)
        }
      } else {
        // Create actors for each thread the new instance wants to call
        val (toCall, resultInstance) = newInstance.handleThreads(handleThread(newInstance))

        // Get the calls' Futures
        val futureCalls = toCall flatMap resultInstance.piFutureOf

        // Put the resulting instance to the store
        store = store.put(resultInstance)

        // Run each of the new actors
        (toCall zip futureCalls) foreach runThread(resultInstance)
      }
  }

  /**
    * Publish process instance result
    * Publish [[PiFailureNoSuchInstance]] if no such instance found in the store
    *
    * @param id ID of the instance
    * @param ref Thread reference
    * @param res Metadata of the result to post
    */
  final def postResult(id: Int, ref: Int, res: MetadataAtomicProcess.MetadataAtomicResult): Unit = {
    publish(PiEventReturn(id, ref, PiObject.get(res._1), res._2))
    store.get(id) match {
      case None => publish(PiFailureNoSuchInstance(id))
      case Some(i) =>
        if (i.id != id) {
          // This should never happen. We trust the Instance Store!
          System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id)
        } else {
          // Reduce the instance and check for completion
          val newInstance = i.postResult(ref, res._1).reduce
          if (newInstance.completed) {
            // Check for result and publish it (or the failure)
            newInstance.result match {
              case None =>
                publish(PiFailureNoResult(newInstance))
                store = store.del(newInstance.id)
              case Some(result) =>
                publish(PiEventResult(newInstance, result))
                store = store.del(newInstance.id)
            }
          } else {
            // Create actors for each thread the new instance wants to call
            val (toCall, resultInstance) = newInstance.handleThreads(handleThread(newInstance))

            // Get the calls' Futures
            val futureCalls = toCall flatMap resultInstance.piFutureOf

            // Put the resulting instance to the store
            store = store.put(resultInstance)

            // Run each of the new actors
            (toCall zip futureCalls) foreach runThread(resultInstance)
          }
        }
    }
  }

  def handleThread(i: PiInstance[Int])
                  (ref: Int, f: PiFuture): Boolean = {
    //TODO Isn't the first match redundant, given the type of f is given as PiFuture in the argument list?
    //  Is it being used to name components of f?
    f match {
      case PiFuture(name, _, _) =>
        i.getProc(name) match {
          case None =>
            publish(PiFailureUnknownProcess(i, name))
            false
          case Some(_: MetadataAtomicProcess) => true
          case Some(_: CompositeProcess) =>
            // Composite processes should not be encountered here
            publish(PiFailureAtomicProcessIsComposite(i, name))
            false
        }
    }
  }

  def runThread(i: PiInstance[Int])
               (t: (Int, PiFuture)): Unit = {
    //TODO Isn't the first match redundant, given the type of t.second is given as PiFuture in the argument list?
    // Is it being used to name components of t?
    t match {
      case (ref, PiFuture(name, _, args)) =>
        i.getProc(name) match {
          case None =>
            // This should never happen! We already checked!
            System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name + " even though we checked already")
          case Some(p: MetadataAtomicProcess) =>
            // Prepare timeout and forget types of arguments
            implicit val tOut: Timeout = Timeout(1.second)
            val objs = args map (_.obj)

            // Create a new actor for the atomic process and tell it about the call
            try {
              publish(PiEventCall(i.id, ref, p, objs))
              // TODO Change from ! to ? to require an acknowledgement
              system.actorOf(AkkaExecutor.atomicprops()) ! AkkaExecutor.ACall(i.id, ref, p, objs, self)
            } catch {
              case _: Throwable => Unit //TODO specify timeout exception here! - also print a warning
            }
          case Some(_: CompositeProcess) =>
            // Composite processes should not be encountered here
            // This is also already checked by handleThread
            publish(PiFailureAtomicProcessIsComposite(i, name))
        }
    }
  }

  def simulationReady(): Boolean = store.simulationReady

  def receive: PartialFunction[Any, Unit] = {
    case AkkaExecutor.Init(p, args) => sender() ! init(p, args)
    case AkkaExecutor.Start(id) => start(id)
    case AkkaExecutor.Result(id, ref, res) => postResult(id, ref, res)
    case AkkaExecutor.Error(id, ref, ex) =>
      publish( PiFailureAtomicProcessException(id, ref, ex) )
      store = store.del(id)
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
    case AkkaExecutor.AckCall => Unit
    case AkkaExecutor.SimReady => sender() ! simulationReady()
    case AkkaExecutor.Subscribe(h) => subscribe(h) pipeTo sender()
    case AkkaExecutor.Unsubscribe(name) => unsubscribe(name) pipeTo sender()
    case m => System.err.println("!!! Received unknown message: " + m)
  }
}

class AkkaAtomicProcessExecutor(implicit val exc: ExecutionContext = ExecutionContext.global)
  extends Actor { //(executor: ActorRef, p: PiProcess, args: Seq[PiObject])

   def receive: PartialFunction[Any, Unit] = {
    case AkkaExecutor.ACall(id, ref, p, args, actor) =>
      //System.err.println("*** [" + id + "] Calling atomic process: " + p.name + " ref:" + ref)
      p.runMeta(args).onComplete{
        case Success(res) => actor ! AkkaExecutor.Result(id, ref, res)
        case Failure(ex) => actor ! AkkaExecutor.Error(id, ref, ex)
      }
      actor ! AkkaExecutor.AckCall
    case m => System.err.println("!! Received unknown message: " + m)
  }
}
