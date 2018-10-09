package com.workflowfm.pew.execution

import com.workflowfm.pew._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout

class AkkaPiEventHandler(handler:PiEventHandler[Int]) extends Actor {
  def receive = {
    case AkkaExecutor.E(event) => if (handler(event)) unsubscribe(context)
    case AkkaExecutor.Unsubscribe(name:String) => if (name == handler.name) unsubscribe(context)
  }
  
  def unsubscribe(context:ActorContext) = {
    context.system.eventStream.unsubscribe(self,classOf[AkkaExecutor.Event])
    context.stop(self)
  }
}

trait AkkaPiObservable extends PiObservable[Int] {
  implicit val system:ActorSystem
  override def subscribe(handler:PiEventHandler[Int]):Unit = {
    val handlerActor = system.actorOf(AkkaExecutor.handlerprops(handler))
    system.eventStream.subscribe(handlerActor, classOf[AkkaExecutor.Event])
  }
  override def unsubscribe(name:String):Unit = {
    system.eventStream.publish(AkkaExecutor.Unsubscribe(name))
  }
}


class AkkaExecutor(store:PiInstanceStore[Int], processes:PiProcessStore)(override implicit val system: ActorSystem, override implicit val context: ExecutionContext = ExecutionContext.global, implicit val timeout:FiniteDuration = 10.seconds) extends ProcessExecutor[Int] with AkkaPiObservable {
  def this(store:PiInstanceStore[Int], l:PiProcess*)(implicit system: ActorSystem, context: ExecutionContext, timeout:FiniteDuration) = this(store,SimpleProcessStore(l :_*))
  def this(system: ActorSystem, context: ExecutionContext, timeout:FiniteDuration,l:PiProcess*) = this(SimpleInstanceStore(),SimpleProcessStore(l :_*))(system,context,timeout)
  def this(l:PiProcess*)(implicit system: ActorSystem) = this(SimpleInstanceStore(),SimpleProcessStore(l :_*))(system,ExecutionContext.global,10.seconds)
  
  val execActor = system.actorOf(AkkaExecutor.execprops(store,processes))
  implicit val tOut = Timeout(timeout) 
  
  override def simulationReady = Await.result(execActor ? AkkaExecutor.SimReady,timeout).asInstanceOf[Boolean]
  
  override protected def init(process:PiProcess,args:Seq[PiObject]):Future[Int] = 
    execActor ? AkkaExecutor.Init(process,args) map (_.asInstanceOf[Int])
    
  override protected def start(id:Int) = execActor ! AkkaExecutor.Start(id)
}

object AkkaExecutor {
  case class Init(p:PiProcess,args:Seq[PiObject])
  case class Start(id:Int)
  case class Result(id:Int,ref:Int,res:PiObject)
  case class Error(id:Int,ref:Int,ex:Throwable)
  
  case class ACall(id:Int,ref:Int,p:AtomicProcess,args:Seq[PiObject],actor:ActorRef)
  case object AckCall
  
  case class AFuture(f:Future[Any])
  
  case object Ping
  case object SimReady
  
  sealed trait Event
  case class E(evt:PiEvent[Int]) extends Event
  case class Unsubscribe(name:String) extends Event
  
  def atomicprops(implicit context: ExecutionContext = ExecutionContext.global): Props = Props(new AkkaAtomicProcessExecutor())
  def execprops(store:PiInstanceStore[Int], processes:PiProcessStore)(implicit system: ActorSystem, exc: ExecutionContext): Props = Props(new AkkaExecActor(store,processes))
  def handlerprops(handler:PiEventHandler[Int])(implicit context: ExecutionContext = ExecutionContext.global): Props = Props(new AkkaPiEventHandler(handler))
}

class AkkaExecActor(var store:PiInstanceStore[Int], processes:PiProcessStore)(implicit system: ActorSystem, implicit val exc: ExecutionContext = ExecutionContext.global) extends Actor {
  var ctr:Int = 0

  def publish(evt:PiEvent[Int]) = system.eventStream.publish(AkkaExecutor.E(evt))
  
  def init(p:PiProcess,args:Seq[PiObject]):Int = { 
	  val inst = PiInstance(ctr,p,args:_*)
	  store = store.put(inst)
	  ctr = ctr + 1
	  ctr-1
  }
  
  def start(id:Int):Unit = store.get(id) match {
    case None => publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
    case Some(inst) => {
      publish(PiEventStart(inst))
  	  val ni = inst.reduce
      if (ni.completed) ni.result match {
  		  case None => {
  			  publish(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))
  			  store = store.del(id)
  		  }
  		  case Some(res) => {
  			  publish(PiEventResult(ni, res))
  			  store = store.del(id)
  		  }
  	  } else {
  		  val (toCall,resi) = ni.handleThreads(handleThread(ni))
  		  val futureCalls = toCall flatMap (resi.piFutureOf)
  			//System.err.println("*** [" + ctr + "] Updating state after init")
  			store = store.put(resi)
  			(toCall zip futureCalls) map runThread(resi)
  	  }
    }
  }
  
  final def postResult(id:Int,ref:Int, res:PiObject):Unit = {
    publish(PiEventReturn(id,ref,res))
    store.get(id) match {
      case None => publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
      case Some(i) => 
        if (i.id != id) System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id) // This should never happen. We trust the Instance Store!
        else {
          //System.err.println("*** [" + id + "] Running!")
          val ni = i.postResult(ref, res).reduce
    		  if (ni.completed) ni.result match {
      		  case None => {
      			  publish(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))
      			  store = store.del(ni.id)
      		  }
      		  case Some(res) => {
      			  publish(PiEventResult(ni, res))
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
        publish(PiEventFailure(i,new ProcessExecutor.UnknownProcessException(name)))
        false
      }
      case Some(p:AtomicProcess) => true
      case Some(p:CompositeProcess) => {  // TODO this should never happen!
        publish(PiEventFailure(i,new ProcessExecutor.AtomicProcessIsCompositeException(name)))
        false 
      }
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
        val objs = args map (_.obj)
        try {
          publish(PiEventCall(i.id,ref,p,objs))
          // TODO Change from ! to ? to require an acknowledgement
          system.actorOf(AkkaExecutor.atomicprops()) ! AkkaExecutor.ACall(i.id,ref,p,objs,self)
        } catch {
          case _:Throwable => Unit //TODO specify timeout exception here! - also print a warning
        }
      }
      case Some(p:CompositeProcess) => {// This should never happen! We already checked! 
        publish(PiEventFailure(i,new ProcessExecutor.AtomicProcessIsCompositeException(name)))
      }
    }
  } }
 
  def simulationReady():Boolean = store.simulationReady
  
  def receive = {
    case AkkaExecutor.Init(p,args) => sender() ! init(p,args)
    case AkkaExecutor.Start(id) => start(id)
    case AkkaExecutor.Result(id,ref,res) => postResult(id,ref,res) 
    case AkkaExecutor.Error(id,ref,ex) => {
      publish(PiEventProcessException(id,ref,ex))
      store = store.del(id)
    }
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
    case AkkaExecutor.AckCall => Unit
    case AkkaExecutor.SimReady => sender() ! simulationReady()
    case m => System.err.println("!!! Received unknown message: " + m)
  }

}

class AkkaAtomicProcessExecutor(implicit val exc: ExecutionContext = ExecutionContext.global) extends Actor { //(executor:ActorRef,p:PiProcess,args:Seq[PiObject])
   def receive = {
    case AkkaExecutor.ACall(id,ref,p,args,actor) => {
      //System.err.println("*** [" + id + "] Calling atomic process: " + p.name + " ref:" + ref)
      p.run(args).onComplete{ 
        case Success(res) => actor ! AkkaExecutor.Result(id,ref,res) 
        case Failure(ex) => actor ! AkkaExecutor.Error(id,ref,ex)  
      }
      actor ! AkkaExecutor.AckCall
    }
    case m => System.err.println("!! Received unknown message: " + m)
  }
}
