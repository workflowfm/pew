package com.workflowfm.pew.execution

import akka.actor._
import akka.event.LoggingReceive
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.workflowfm.pew._
import com.workflowfm.pew.stream.{ PiObservable, PiStream, PiEventHandler, PiSwitch }

import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import java.util.UUID

class AkkaExecutor (
  store:PiInstanceStore[UUID]
)(
  implicit val system: ActorSystem,
  implicit val timeout: FiniteDuration
) extends ProcessExecutor[UUID] with PiObservable[UUID] {

  implicit val tag: ClassTag[UUID] = ClassTag(classOf[UUID])
  override implicit val executionContext: ExecutionContext = system.dispatcher

  def this()(implicit system: ActorSystem, timeout: FiniteDuration = 10.seconds) =
    this(SimpleInstanceStore[UUID]())(system,timeout)

  val execActor = system.actorOf(AkkaExecutor.execprops(store))
  implicit val tOut = Timeout(timeout)

  override protected def init(instance: PiInstance[_]): Future[UUID] =
    execActor ? AkkaExecutor.Init(instance) map (_.asInstanceOf[UUID])

  override protected def start(id: UUID) = execActor ! AkkaExecutor.Start(id)

  override def subscribe(handler: PiEventHandler[UUID]): Future[PiSwitch] = (execActor ? AkkaExecutor.Subscribe(handler)).mapTo[PiSwitch]

}

object AkkaExecutor {
  case class Init(instance: PiInstance[_])
  case class Start(id:UUID)
  case class Result(id:UUID,ref:Int,res:MetadataAtomicProcess.MetadataAtomicResult)
  case class Error(id:UUID,ref:Int,ex:Throwable)
  
  case class ACall(id:UUID,ref:Int,p:MetadataAtomicProcess,args:Seq[PiObject],actor:ActorRef)
  case object AckCall
  
  case class AFuture(f:Future[Any])
  
  case object Ping
  
  case class Subscribe(handler:PiEventHandler[UUID])

  def execprops(store: PiInstanceStore[UUID])
    (implicit system: ActorSystem, timeout: FiniteDuration): Props = Props(
    new AkkaExecActor(store)(system.dispatcher, implicitly[ClassTag[PiEvent[UUID]]], timeout)
  )
}

class AkkaExecActor(
  var store:PiInstanceStore[UUID],
)(
  implicit val executionContext: ExecutionContext,
  override implicit val tag: ClassTag[PiEvent[UUID]],
  override implicit val timeout: FiniteDuration
) extends Actor with PiStream[UUID] {

  def init(instance: PiInstance[_]): UUID = {
    val id = java.util.UUID.randomUUID
    store = store.put(instance.copy(id = id))
    id
  }
  
  def start(id:UUID):Unit = store.get(id) match {
    case None => publish(PiFailureNoSuchInstance(id))
    case Some(inst) => {
      publish(PiEventStart(inst))
  	  val ni = inst.reduce
      if (ni.completed) ni.result match {
  		case None => {
  		  publish(PiFailureNoResult(ni))
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
        publish(PiEventIdle(resi))
  	  }
    }
  }
  
  final def postResult(id:UUID, ref:Int, res:MetadataAtomicProcess.MetadataAtomicResult):Unit = {
    publish(PiEventReturn(id,ref,PiObject.get(res._1),res._2))
    store.get(id) match {
      case None => publish(PiFailureNoSuchInstance(id))
      case Some(i) =>
        if (i.id != id) System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id) // This should never happen. We trust the Instance Store!
        else {
          //System.err.println("*** [" + id + "] Running!")
          val ni = i.postResult(ref, res._1).reduce
    	  if (ni.completed) ni.result match {
      		case None => {
      		  publish(PiFailureNoResult(ni))
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
            publish(PiEventIdle(resi))
    	  }
        }
	}
  }
  
  def handleThread(i:PiInstance[UUID])(ref:Int,f:PiFuture):Boolean = {
    //System.err.println("*** [" + id + "] Checking thread: " + ref + " (" + f.fun + ")")
    f match {
      case PiFuture(name, outChan, args) => i.getProc(name) match {
        case None => {
          publish(PiFailureUnknownProcess(i, name))
          false
        }
        case Some(p:MetadataAtomicProcess) => true
        case Some(p:CompositeProcess) => {  // TODO this should never happen!
          publish(PiFailureAtomicProcessIsComposite(i, name))
          false
        }
      }
    } }
  //
  
  def runThread(i:PiInstance[UUID])(t:(Int,PiFuture)):Unit = {
    //System.err.println("*** [" + id + "] Running thread: " + t._1 + " (" + t._2.fun + ")")
    t match {
      case (ref,PiFuture(name, outChan, args)) => i.getProc(name) match {
        case None => {
          // This should never happen! We already checked!
          System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name + " even though we checked already")
        }
        case Some(p:MetadataAtomicProcess) => {
          implicit val tOut = Timeout(1.second)
          val objs = args map (_.obj)
          try {
            publish(PiEventCall(i.id,ref,p,objs))
            p.runMeta(objs).onComplete{
              case Success(res) => self ! AkkaExecutor.Result(i.id,ref,res)
              case Failure(ex) => self ! AkkaExecutor.Error(i.id,ref,ex)
            }
          } catch {
            case _:Throwable => Unit //TODO specify timeout exception here! - also print a warning
          }
        }
        case Some(p:CompositeProcess) => {// This should never happen! We already checked!
          publish(PiFailureAtomicProcessIsComposite(i, name))
        }
      }
    } }

  def akkaReceive: Receive = {
    case AkkaExecutor.Init(inst) => sender() ! init(inst)
    case AkkaExecutor.Start(id) => start(id)
    case AkkaExecutor.Result(id,ref,res) => postResult(id,ref,res)
    case AkkaExecutor.Error(id,ref,ex) => {
      publish( PiFailureAtomicProcessException(id,ref,ex) )
      store = store.del(id)
    }
    case AkkaExecutor.Ping => sender() ! AkkaExecutor.Ping
    case AkkaExecutor.AckCall => Unit
    case AkkaExecutor.Subscribe(h) => subscribe(h) pipeTo sender()
    case m => System.err.println("!!! Received unknown message: " + m)
  }

  override def receive = LoggingReceive { publisherBehaviour orElse akkaReceive }
}
