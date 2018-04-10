package com.workflowfm.pew.execution

import com.workflowfm.pew._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout


class AkkaExecutor(store:PiInstanceStore[Int], processes:PiProcessStore)(implicit system: ActorSystem, override implicit val context: ExecutionContext = ExecutionContext.global, implicit val timeout:FiniteDuration = 10.seconds) extends FutureExecutor {
  def this(store:PiInstanceStore[Int], l:PiProcess*)(implicit system: ActorSystem, context: ExecutionContext, timeout:FiniteDuration) = this(store,SimpleProcessStore(l :_*))
  def this(system: ActorSystem, context: ExecutionContext, timeout:FiniteDuration,l:PiProcess*) = this(SimpleInstanceStore(),SimpleProcessStore(l :_*))(system,context,timeout)
  def this(l:PiProcess*)(implicit system: ActorSystem) = this(SimpleInstanceStore(),SimpleProcessStore(l :_*))(system,ExecutionContext.global,10.seconds)
  
  val execActor = system.actorOf(AkkaExecutor.execprops(store,processes))
  implicit val tOut = Timeout(timeout) 
  
  override def execute(process:PiProcess,args:Seq[Any]):Future[Future[Any]] = 
    Future.successful((execActor ? AkkaExecutor.Call(process,args map PiObject.apply)))
}

object AkkaExecutor {
  case class Call(p:PiProcess,args:Seq[PiObject])
  case class Result(id:Int,ref:Int,res:PiObject)
  case class Error(id:Int,ref:Int,ex:Throwable)
  
  case class ACall(id:Int,ref:Int,p:AtomicProcess,args:Seq[PiObject],actor:ActorRef)
  case object AckCall
  
  case class AFuture(f:Future[Any])
  
  case object Ping
  
  def atomicprops(implicit context: ExecutionContext = ExecutionContext.global): Props = Props(new AkkaAtomicProcessExecutor())
  def execprops(store:PiInstanceStore[Int], processes:PiProcessStore)(implicit system: ActorSystem, exc: ExecutionContext): Props = Props(new AkkaExecActor(store,processes))
}

class AkkaExecActor(var store:PiInstanceStore[Int], processes:PiProcessStore)(implicit system: ActorSystem, implicit val exc: ExecutionContext = ExecutionContext.global) extends Actor {
  var ctr:Int = 0
  val handler = new PromiseHandler[Int]
   
  def call(p:PiProcess,args:Seq[PiObject]) = {
    //System.err.println("*** [" + ctr + "] Starting call of:" + p.name)
	  val inst = PiInstance(ctr,p,args:_*)
	  val ret = handler.start(inst)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))
		  }
		  case Some(res) => {
			  handler.success(ni, res)
		  }
	  } else {
		  val (toCall,resi) = ni.handleThreads(handleThread(ni))
		  val futureCalls = toCall flatMap (resi.piFutureOf)
			//System.err.println("*** [" + ctr + "] Updating state after init")
			store = store.put(resi)
			(toCall zip futureCalls) map runThread(resi)
	  }
	  ctr = ctr + 1
	  //System.err.println("*** [" + (ctr-1) + "] Done init.")
	  ret
  }
  
  final def postResult(id:Int,ref:Int, res:PiObject):Unit = {
    //System.err.println("*** [" + id + "] Received result for thread " + ref + " : " + res)
    store.get(id) match {
      case None => System.err.println("*** [" + id + "] No running instance! ***")
      case Some(i) => 
        if (i.id != id) System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id)
        else {
          //System.err.println("*** [" + id + "] Running!")
          val ni = i.postResult(ref, res).reduce
    		  if (ni.completed) ni.result match {
      		  case None => {
      			  handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))
      			  store = store.del(ni.id)
      		  }
      		  case Some(res) => {
      			  handler.success(ni, res)
      			  store = store.del(ni.id)
      		  }
    		  } else {
    		    val (toCall,resi) = ni.handleThreads(handleThread(ni))
		        val futureCalls = toCall flatMap (resi.piFutureOf)
		        //System.err.println("*** [" + i.id + "] Updating state after: " + ref)
			      store = store.put(resi)
		        (toCall zip futureCalls) map runThread(resi)
    		  }
        }
	  }
  }
 
  def handleThread(i:PiInstance[Int])(ref:Int,f:PiFuture):Boolean = {
    //System.err.println("*** [" + id + "] Checking thread: " + ref + " (" + f.fun + ")")
    f match {
    case PiFuture(name, outChan, args) => i.getProc(name) match {
      case None => {
        //System.err.println("*** [" + id + "] ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => true
      case Some(p:CompositeProcess) => { System.err.println("*** [" + i.id + "] Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  } }
    //
  
  def runThread(i:PiInstance[Int])(t:(Int,PiFuture)):Unit = {
    //System.err.println("*** [" + id + "] Running thread: " + t._1 + " (" + t._2.fun + ")")
    t match {
    case (ref,PiFuture(name, outChan, args)) => i.getProc(name) match {
      case None => {
        // This should never happen! We already checked!
        System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name + " even though we checked already")
      }
      case Some(p:AtomicProcess) => {
        implicit val tOut = Timeout(1.second)
        try {
          // TODO Change from ! to ? to require an acknowledgement
          system.actorOf(AkkaExecutor.atomicprops()) ! AkkaExecutor.ACall(i.id,ref,p,args map (_.obj),self)
          //println("OKOKOK " + p.name)
        } catch {
          case _:Throwable => Unit //TODO specify timeout exception here! - also print a warning
        }
        //System.err.println("*** [" + id + "] Requested call of process: " + p.name + " ref:" + ref)
      }
      case Some(p:CompositeProcess) => {// This should never happen! We already checked! 
        System.err.println("*** [" + i.id + "] Executor encountered composite process thread: " + name)
      }
    }
  } }
 
  def receive = {
    case AkkaExecutor.Call(p,args) => call(p,args) pipeTo sender()
    case AkkaExecutor.Result(id,ref,res) => postResult(id,ref,res) 
    case AkkaExecutor.Error(id,ref,ex) => handler.failure(id,ex)
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
    case AkkaExecutor.AckCall => Unit
    case m => System.err.println("!!! Received unknown message: " + m)
  }

}

class AkkaAtomicProcessExecutor(implicit val exc: ExecutionContext = ExecutionContext.global) extends Actor { //(executor:ActorRef,p:PiProcess,args:Seq[PiObject])
   def receive = {
    case AkkaExecutor.ACall(id,ref,p,args,actor) => {
      //System.err.println("*** [" + id + "] Calling atomic process: " + p.name + " ref:" + ref)
      //val actor = sender()
      p.run(args).onComplete{ 
        case Success(res) => actor ! AkkaExecutor.Result(id,ref,res) 
        case Failure(ex) => actor ! AkkaExecutor.Error(id,ref,ex)  
      }
      actor ! AkkaExecutor.AckCall
    }
    case m => System.err.println("!! Received unknown message: " + m)
  }
}
