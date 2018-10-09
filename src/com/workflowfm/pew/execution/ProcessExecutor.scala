package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.Map

/**
 * Executes an atomic process - blocking
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
  protected def init(process:PiProcess,args:Seq[PiObject]):Future[KeyT]
  protected def start(id:KeyT):Unit
  def simulationReady:Boolean
  
  implicit val context: ExecutionContext //= ExecutionContext.global

	def call(process:PiProcess,args:Seq[Any]):Future[KeyT] = {
    init(process,args map PiObject.apply) map { id => start(id) ; id }
  }
	
	def call[H <: PiEventHandler[KeyT]](process:PiProcess,args:Seq[Any],factory:PiEventHandlerFactory[KeyT,H]):Future[H] = {
	   init(process,args map PiObject.apply) map { id => 
	     val handler = factory.build(id) 
	     subscribe(handler) 
	     start(id)
	     handler
	   }
  }
	
  def execute(process:PiProcess,args:Seq[Any]):Future[Any] = 
    call(process,args,new PromiseHandlerFactory[KeyT]({ id => "["+id+"]"})) flatMap (_.future)
}

object ProcessExecutor {
  //def default = SingleBlockingExecutor(Map[String,PiProcess]())
  
  final case class AlreadyExecutingException(private val cause: Throwable = None.orNull)
                    extends Exception("Unable to execute more than one process at a time", cause) 
  final case class UnknownProcessException(val process:String, private val cause: Throwable = None.orNull)
                    extends Exception("Unknown process: " + process, cause) 
  final case class AtomicProcessIsCompositeException(val process:String, private val cause: Throwable = None.orNull)
                    extends Exception("Executor encountered composite process thread: " + process + " (this should never happen!)", cause)
  final case class NoResultException(val id:String, private val cause: Throwable = None.orNull)
                    extends Exception("Failed to get result for: " + id, cause) 
  final case class NoSuchInstanceException(val id:String, private val cause: Throwable = None.orNull)
                    extends Exception("Failed to find instance with id: " + id, cause) 
}

/**
 * Shortcut methods for unit testing
 */
trait ProcessExecutorTester {
  def exe(e:ProcessExecutor[_],p:PiProcess,args:Any*) = await(e.execute(p,args:Seq[Any]))
  def await[A](f:Future[A]):A = try {
    Await.result(f,15.seconds)
  } catch {
    case e:Throwable => {
      System.out.println("=== RESULT FAILED! ===")
      throw e
    }
  }
  
//  def awaitf[A](f:Future[Future[A]]):A = try {
//    Await.result(Await.result(f,15.seconds),15.seconds)
//  } catch {
//    case e:Throwable => {
//      System.out.println("=== RESULT FAILED! ===")
//      throw e
//    }
//  }
}
